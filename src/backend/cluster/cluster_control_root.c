/*-------------------------------------------------------------------------
 *
 * cluster_control_root.c
 *	  Survivor-readable failed-origin control-root carrier (RF-ROOT P1).
 *
 * The carrier is the user-approved PGRAC adaptation in frozen private spec
 * Stage 8 contract section 17.  It does not claim that these
 * bytes are Oracle control-file bytes.  Oracle alignment is at the authority
 * boundary: shared durable control metadata serialized by the CF enqueue.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_external_fence.h"
#include "cluster/cluster_ir.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_qvotec.h"
#include "cluster/cluster_semantic_activation.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_wal_claim.h"
#include "cluster/cluster_wal_durable_prefix.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_tail.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_write_fence.h"
#include "cluster_control_root_private.h"
#include "cluster_recovery_anchor_private.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "miscadmin.h"
#include "port/pg_crc32c.h"
#include "postmaster/interrupt.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#define CONTROL_ROOT_HEADER_MAGIC "PGCH"
#define CONTROL_ROOT_RECORD_MAGIC "PGRT"
#define CONTROL_ROOT_FORMAT_VERSION UINT16_C(1)
#define CONTROL_ROOT_ENDIAN_TAG UINT32_C(0x01020304)
#define CONTROL_ROOT_READER_VERSION UINT16_C(1)
#define CONTROL_ROOT_WRITER_VERSION UINT16_C(1)
#define CONTROL_ROOT_HEADER_VERSION_V2 UINT16_C(2)
#define CONTROL_ROOT_RECORD_VERSION_V2 UINT16_C(2)
#define CONTROL_ROOT_SOURCE_PRIMARY UINT8_C(1)
#define CONTROL_ROOT_SOURCE_BAK_BLOCKED UINT8_C(2)
#define CONTROL_ROOT_SOURCE_BOOTSTRAP_PRIMARY UINT8_C(3)

#define CONTROL_ROOT_HEADER_CRC_OFFSET 504
#define CONTROL_ROOT_RECORD_CRC_OFFSET 504
#define CONTROL_ROOT_BODY_OFFSET CLUSTER_CONTROL_ROOT_HEADER_BYTES

static uint16
read_u16_le(const uint8 *src)
{
	return (uint16)src[0] | ((uint16)src[1] << 8);
}

static uint32
read_u32_le(const uint8 *src)
{
	return (uint32)src[0] | ((uint32)src[1] << 8) | ((uint32)src[2] << 16) | ((uint32)src[3] << 24);
}

static uint64
read_u64_le(const uint8 *src)
{
	uint64 value = 0;
	int i;

	for (i = 7; i >= 0; i--)
		value = (value << 8) | src[i];
	return value;
}

static void make_file_token(const ControlRootImage *image, ClusterControlRootFileToken *token);

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

static uint32
control_root_crc(const uint8 *bytes, size_t len)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, len);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static bool
control_root_sha256(const uint8 *bytes, size_t len, uint8 digest[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx;
	bool ok = false;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	if (pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, len) >= 0
		&& pg_cryptohash_final(ctx, digest, PG_SHA256_DIGEST_LENGTH) >= 0)
		ok = true;
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
bytes_are_zero(const void *ptr, size_t len)
{
	const uint8 *bytes = ptr;
	size_t i;

	for (i = 0; i < len; i++) {
		if (bytes[i] != 0)
			return false;
	}
	return true;
}

static bool
uuid_v4_valid(const uint8 uuid[16])
{
	return !bytes_are_zero(uuid, 16) && (uuid[6] & UINT8_C(0xf0)) == UINT8_C(0x40)
		   && (uuid[8] & UINT8_C(0xc0)) == UINT8_C(0x80);
}

bool
cluster_control_root_identity_equal(const ClusterControlRootIdentity *left,
									const ClusterControlRootIdentity *right)
{
	return left != NULL && right != NULL && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && memcmp(left->authority_uuid, right->authority_uuid, 16) == 0
		   && left->origin_thread_id == right->origin_thread_id && left->reserved42 == 0
		   && right->reserved42 == 0 && left->origin_node_id == right->origin_node_id
		   && left->thread_claim_created_at == right->thread_claim_created_at
		   && left->thread_claim_crc32c == right->thread_claim_crc32c && left->reserved60 == 0
		   && right->reserved60 == 0
		   && left->origin_owner_incarnation == right->origin_owner_incarnation
		   && left->root_lineage_seq == right->root_lineage_seq;
}

bool
cluster_control_root_feature_bitmap_is_known(uint64 active_feature_bitmap)
{
	return (active_feature_bitmap & ~PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1) == 0;
}

static bool
build_control_path(char *dst, size_t dstlen, const char *relative)
{
	int written;

	if (dst == NULL || dstlen == 0 || relative == NULL || cluster_shared_data_dir == NULL
		|| cluster_shared_data_dir[0] == '\0')
		return false;
	written = snprintf(dst, dstlen, "%s/%s", cluster_shared_data_dir, relative);
	return written > 0 && (size_t)written < dstlen;
}

static bool
regular_or_absent_nosymlink(const char *path, bool allow_absent)
{
	struct stat st;

	if (lstat(path, &st) != 0)
		return allow_absent && errno == ENOENT;
	return S_ISREG(st.st_mode);
}

static bool
control_paths_safe(void)
{
	char global_path[MAXPGPATH];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	struct stat st;

	if (cluster_shared_data_dir == NULL || lstat(cluster_shared_data_dir, &st) != 0
		|| !S_ISDIR(st.st_mode))
		return false;
	if (snprintf(global_path, sizeof(global_path), "%s/global", cluster_shared_data_dir) <= 0
		|| lstat(global_path, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;
	if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return false;
	return regular_or_absent_nosymlink(primary, true) && regular_or_absent_nosymlink(bak, true);
}

static int
hex_digit_value(char ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	return -1;
}

static bool
current_storage_uuid(uint8 uuid[16])
{
	char text[CLUSTER_SHARED_UUID_LEN];
	int i;

	memset(text, 0, sizeof(text));
	cluster_shared_fs_get_storage_uuid(text, sizeof(text));
	if (strlen(text) != 32)
		return false;
	for (i = 0; i < 16; i++) {
		int high = hex_digit_value(text[i * 2]);
		int low = hex_digit_value(text[i * 2 + 1]);

		if (high < 0 || low < 0)
			return false;
		uuid[i] = (uint8)((high << 4) | low);
	}
	return !bytes_are_zero(uuid, 16);
}

static ClusterControlRootResult
storage_contract_check(const uint8 *expected_storage_uuid, bool require_local_probe)
{
	uint8 current_uuid[16];
	ClusterCfContractState state;
	bool multi_node;

	if (!current_storage_uuid(current_uuid))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	if (expected_storage_uuid != NULL && memcmp(current_uuid, expected_storage_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (DataDir == NULL || DataDir[0] == '\0')
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	state = cluster_cf_contract_load(DataDir);
	multi_node = cluster_conf_node_count() > 1;
	if ((multi_node && !cluster_cf_storage_write_allowed(state, true))
		|| (!multi_node && require_local_probe && !cluster_cf_storage_probe_local()))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	if (!control_paths_safe())
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
snapshot_reserved_zero(const ClusterControlRootSnapshot *snapshot)
{
	return snapshot->identity.reserved42 == 0 && snapshot->identity.reserved60 == 0
		   && snapshot->reserved96 == 0 && snapshot->reserved122 == 0 && snapshot->reserved124 == 0
		   && snapshot->reserved160 == 0 && snapshot->reserved208 == 0;
}

static ClusterControlRootResult
snapshot_validate(const ClusterControlRootSnapshot *snapshot, uint16 expected_thread,
				  uint64 system_identifier, const uint8 storage_uuid[16],
				  const uint8 authority_uuid[16], bool v2)
{
	/* PGRAC: v2 validity flags and exact WAL/claim checks prove presence.
	 * Zero is a possible CRC32C, not an absence sentinel. Keep v1 unchanged.
	 * Author: SqlRush <sqlrush@gmail.com>
	 */
	uint32 flags;
	bool checkpoint_valid;
	bool tail_valid;
	bool recovered_valid;
	bool tail_last_valid;
	bool recovered_last_valid;
	bool bound_valid;

	if (snapshot == NULL || !snapshot_reserved_zero(snapshot))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (snapshot->identity.system_identifier != system_identifier
		|| memcmp(snapshot->identity.storage_uuid, storage_uuid, 16) != 0
		|| memcmp(snapshot->identity.authority_uuid, authority_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (snapshot->identity.origin_thread_id != expected_thread
		|| snapshot->identity.origin_node_id < 0
		|| snapshot->identity.origin_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| snapshot->identity.thread_claim_created_at == 0
		|| (!v2 && snapshot->identity.thread_claim_crc32c == 0)
		|| snapshot->identity.origin_owner_incarnation == 0
		|| snapshot->identity.root_lineage_seq == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (snapshot->lifecycle < CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| snapshot->lifecycle > CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED
		|| snapshot->root_publish_seq == 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	flags = snapshot->root_flags;
	if ((flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0
		|| (flags & CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID) == 0)
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	checkpoint_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID) != 0;
	tail_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) != 0;
	recovered_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID) != 0;
	tail_last_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID) != 0;
	recovered_last_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID) != 0;
	bound_valid = (flags & CLUSTER_CONTROL_ROOT_FLAG_CONSERVATIVE_SCN_VALID) != 0;

	if (!checkpoint_valid || snapshot->checkpoint_tli == 0 || snapshot->checkpoint_lower_lsn == 0
		|| (!v2 && snapshot->checkpoint_record_crc32c == 0)
		|| (snapshot->checkpoint_source_kind != CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1
			&& snapshot->checkpoint_source_kind
				   != CLUSTER_CONTROL_ROOT_CHECKPOINT_RECOVERY_ANCHOR_V1))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (tail_valid) {
		if (snapshot->tail_tli == 0
			|| snapshot->tail_validation_kind != CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1
			|| snapshot->validated_tail_lsn_exclusive < snapshot->checkpoint_lower_lsn)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (snapshot->validated_tail_lsn_exclusive == snapshot->checkpoint_lower_lsn) {
			if (tail_last_valid || snapshot->tail_last_record_lsn != 0
				|| snapshot->tail_last_record_crc32c != 0)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		} else if (!tail_last_valid || snapshot->tail_last_record_lsn == 0
				   || (!v2 && snapshot->tail_last_record_crc32c == 0)
				   || snapshot->tail_last_record_lsn >= snapshot->validated_tail_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	} else if (snapshot->tail_tli != 0 || snapshot->tail_validation_kind != 0
			   || snapshot->validated_tail_lsn_exclusive != 0 || tail_last_valid
			   || snapshot->tail_last_record_lsn != 0 || snapshot->tail_last_record_crc32c != 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;

	if (recovered_valid) {
		if (!tail_valid || snapshot->recovered_tli == 0
			|| snapshot->recovered_through_lsn_exclusive < snapshot->checkpoint_lower_lsn
			|| snapshot->recovered_through_lsn_exclusive > snapshot->validated_tail_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (snapshot->recovered_through_lsn_exclusive == snapshot->checkpoint_lower_lsn) {
			if (recovered_last_valid || snapshot->recovered_last_record_lsn != 0
				|| snapshot->recovered_last_record_crc32c != 0)
				return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		} else if (!recovered_last_valid || snapshot->recovered_last_record_lsn == 0
				   || (!v2 && snapshot->recovered_last_record_crc32c == 0)
				   || snapshot->recovered_last_record_lsn
						  >= snapshot->recovered_through_lsn_exclusive)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	} else if (snapshot->recovered_tli != 0 || recovered_last_valid
			   || snapshot->recovered_last_record_lsn != 0
			   || snapshot->recovered_last_record_crc32c != 0
			   || (snapshot->recovered_through_lsn_exclusive != 0
				   && snapshot->recovered_through_lsn_exclusive != snapshot->checkpoint_lower_lsn))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;

	if (bound_valid) {
		if (snapshot->conservative_bound_kind != CLUSTER_CONTROL_ROOT_BOUND_R14_M1_PARTITION_S_V1
			|| snapshot->conservative_commit_scn == 0)
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	} else if (snapshot->conservative_bound_kind != CLUSTER_CONTROL_ROOT_BOUND_NONE
			   || snapshot->conservative_commit_scn != 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (snapshot->lifecycle_reason < CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT
		|| snapshot->lifecycle_reason > CLUSTER_CONTROL_ROOT_PUBLISH_COPY_REPAIR)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: explicit v2 extensions; legacy callers retain strict v1 wrappers.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
common_v2_validate(const ControlRootCommonV2 *common)
{
	if (common->reserved4 != 0)
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (common->database_state < CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| common->database_state > CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (common->database_incarnation == 0 || common->formation_seq == 0
		|| (common->configured[0] == 0 && common->configured[1] == 0)
		|| (common->serving[0] & ~common->configured[0]) != 0
		|| (common->serving[1] & ~common->configured[1]) != 0 || common->config_generation == 0
		|| bytes_are_zero(common->config_sha256, 32) || common->control_image_generation == 0
		|| bytes_are_zero(common->control_image_sha256, 32)
		|| common->catalog_manifest_generation == 0
		|| bytes_are_zero(common->catalog_manifest_sha256, 32)
		|| common->global_scn_high_water == 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
record_refs_v2_validate(const ControlRootRecordRefsV2 *refs)
{
	if ((refs->history_generation == 0) != bytes_are_zero(refs->history_sha256, 32)
		|| refs->anchor_generation == 0 || bytes_are_zero(refs->anchor_sha256, 32)
		|| bytes_are_zero(refs->claim_sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
encode_record_version(uint8 *dst, const ClusterControlRootSnapshot *snapshot,
					  uint64 publisher_incarnation, uint32 publisher_node,
					  ClusterControlRootPublishReason reason, uint16 version,
					  const ControlRootRecordRefsV2 *refs)
{
	memset(dst, 0, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	memcpy(dst, CONTROL_ROOT_RECORD_MAGIC, 4);
	write_u16_le(dst + 4, version);
	write_u16_le(dst + 6, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u16_le(dst + 8, snapshot->identity.origin_thread_id);
	dst[10] = (uint8)snapshot->lifecycle;
	write_u32_le(dst + 12, (uint32)snapshot->identity.origin_node_id);
	write_u64_le(dst + 16, snapshot->root_publish_seq);
	write_u64_le(dst + 24, snapshot->identity.root_lineage_seq);
	write_u64_le(dst + 32, snapshot->identity.system_identifier);
	memcpy(dst + 40, snapshot->identity.storage_uuid, 16);
	memcpy(dst + 56, snapshot->identity.authority_uuid, 16);
	write_u64_le(dst + 72, (uint64)snapshot->identity.thread_claim_created_at);
	write_u64_le(dst + 80, snapshot->identity.origin_owner_incarnation);
	write_u32_le(dst + 96, snapshot->checkpoint_tli);
	write_u32_le(dst + 100, snapshot->tail_tli);
	write_u32_le(dst + 104, snapshot->recovered_tli);
	write_u32_le(dst + 108, snapshot->root_flags);
	write_u64_le(dst + 112, snapshot->checkpoint_lower_lsn);
	write_u64_le(dst + 120, snapshot->validated_tail_lsn_exclusive);
	write_u64_le(dst + 128, snapshot->recovered_through_lsn_exclusive);
	write_u64_le(dst + 136, snapshot->conservative_commit_scn);
	write_u64_le(dst + 144, publisher_incarnation);
	write_u32_le(dst + 152, publisher_node);
	write_u32_le(dst + 156, (uint32)reason);
	write_u64_le(dst + 160, (uint64)snapshot->published_at_usec);
	write_u32_le(dst + 168, snapshot->identity.thread_claim_crc32c);
	write_u32_le(dst + 172, snapshot->checkpoint_record_crc32c);
	write_u64_le(dst + 176, snapshot->tail_last_record_lsn);
	write_u32_le(dst + 184, snapshot->tail_last_record_crc32c);
	write_u32_le(dst + 188, snapshot->recovered_last_record_crc32c);
	write_u16_le(dst + 192, snapshot->tail_validation_kind);
	write_u16_le(dst + 194, snapshot->checkpoint_source_kind);
	write_u16_le(dst + 196, snapshot->conservative_bound_kind);
	write_u64_le(dst + 208, snapshot->recovered_last_record_lsn);
	if (version == CONTROL_ROOT_RECORD_VERSION_V2) {
		write_u64_le(dst + 216, refs->history_generation);
		memcpy(dst + 224, refs->history_sha256, 32);
		write_u64_le(dst + 256, refs->anchor_generation);
		memcpy(dst + 264, refs->anchor_sha256, 32);
		memcpy(dst + 296, refs->claim_sha256, 32);
	}
	write_u32_le(dst + CONTROL_ROOT_RECORD_CRC_OFFSET,
				 control_root_crc(dst, CONTROL_ROOT_RECORD_CRC_OFFSET));
}

static void
encode_record(uint8 *dst, const ClusterControlRootSnapshot *snapshot, uint64 publisher_incarnation,
			  uint32 publisher_node, ClusterControlRootPublishReason reason)
{
	encode_record_version(dst, snapshot, publisher_incarnation, publisher_node, reason,
						  CONTROL_ROOT_FORMAT_VERSION, NULL);
}

static ClusterControlRootResult
decode_record(const uint8 *src, uint16 expected_thread, const ControlRootHeader *header,
			  ClusterControlRootSnapshot *snapshot, uint32 *crc_out, uint16 expected_version,
			  ControlRootRecordRefsV2 *refs)
{
	ClusterControlRootResult result;
	uint64 publisher_incarnation;
	uint32 publisher_node;
	uint32 reason;
	uint32 stored_crc;

	memset(snapshot, 0, sizeof(*snapshot));
	if (bytes_are_zero(src, CLUSTER_CONTROL_ROOT_RECORD_BYTES)) {
		*crc_out = 0;
		return CLUSTER_CONTROL_ROOT_ABSENT;
	}
	if (memcmp(src, CONTROL_ROOT_RECORD_MAGIC, 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(src + 4) != expected_version
		|| read_u16_le(src + 6) != CLUSTER_CONTROL_ROOT_RECORD_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	stored_crc = read_u32_le(src + CONTROL_ROOT_RECORD_CRC_OFFSET);
	if (stored_crc != control_root_crc(src, CONTROL_ROOT_RECORD_CRC_OFFSET))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	if (src[11] != 0 || !bytes_are_zero(src + 88, 8) || !bytes_are_zero(src + 198, 10)
		|| !bytes_are_zero(src + (expected_version == CONTROL_ROOT_RECORD_VERSION_V2 ? 328 : 216),
						   expected_version == CONTROL_ROOT_RECORD_VERSION_V2 ? 176 : 288)
		|| !bytes_are_zero(src + 508, 4))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (read_u16_le(src + 8) != expected_thread)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;

	snapshot->identity.system_identifier = read_u64_le(src + 32);
	memcpy(snapshot->identity.storage_uuid, src + 40, 16);
	memcpy(snapshot->identity.authority_uuid, src + 56, 16);
	snapshot->identity.origin_thread_id = read_u16_le(src + 8);
	snapshot->identity.origin_node_id = (int32)read_u32_le(src + 12);
	snapshot->identity.thread_claim_created_at = (int64)read_u64_le(src + 72);
	snapshot->identity.thread_claim_crc32c = read_u32_le(src + 168);
	snapshot->identity.origin_owner_incarnation = read_u64_le(src + 80);
	snapshot->identity.root_lineage_seq = read_u64_le(src + 24);
	snapshot->lifecycle = src[10];
	snapshot->root_flags = read_u32_le(src + 108);
	snapshot->root_publish_seq = read_u64_le(src + 16);
	snapshot->checkpoint_tli = read_u32_le(src + 96);
	snapshot->tail_tli = read_u32_le(src + 100);
	snapshot->recovered_tli = read_u32_le(src + 104);
	snapshot->checkpoint_source_kind = read_u16_le(src + 194);
	snapshot->tail_validation_kind = read_u16_le(src + 192);
	snapshot->conservative_bound_kind = read_u16_le(src + 196);
	snapshot->checkpoint_lower_lsn = read_u64_le(src + 112);
	snapshot->validated_tail_lsn_exclusive = read_u64_le(src + 120);
	snapshot->recovered_through_lsn_exclusive = read_u64_le(src + 128);
	snapshot->conservative_commit_scn = read_u64_le(src + 136);
	snapshot->tail_last_record_lsn = read_u64_le(src + 176);
	snapshot->recovered_last_record_lsn = read_u64_le(src + 208);
	snapshot->published_at_usec = (int64)read_u64_le(src + 160);
	snapshot->tail_last_record_crc32c = read_u32_le(src + 184);
	snapshot->checkpoint_record_crc32c = read_u32_le(src + 172);
	snapshot->recovered_last_record_crc32c = read_u32_le(src + 188);
	snapshot->lifecycle_reason = read_u32_le(src + 156);

	publisher_incarnation = read_u64_le(src + 144);
	publisher_node = read_u32_le(src + 152);
	reason = read_u32_le(src + 156);
	if (publisher_incarnation == 0 || publisher_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| reason < CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT
		|| reason > CLUSTER_CONTROL_ROOT_PUBLISH_COPY_REPAIR)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	result = snapshot_validate(snapshot, expected_thread, header->system_identifier,
							   header->storage_uuid, header->authority_uuid,
							   expected_version == CONTROL_ROOT_RECORD_VERSION_V2);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (expected_version == CONTROL_ROOT_RECORD_VERSION_V2) {
		if (snapshot->identity.origin_node_id != (int32)expected_thread - 1)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		refs->history_generation = read_u64_le(src + 216);
		memcpy(refs->history_sha256, src + 224, 32);
		refs->anchor_generation = read_u64_le(src + 256);
		memcpy(refs->anchor_sha256, src + 264, 32);
		memcpy(refs->claim_sha256, src + 296, 32);
		result = record_refs_v2_validate(refs);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	*crc_out = stored_crc;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: retained-writer codec; Author: SqlRush <sqlrush@gmail.com>. */
static bool
history_overlaps_output(const void *input, size_t len, const ClusterWalHistoryImage *out)
{
	uintptr_t a = (uintptr_t)input, b = (uintptr_t)out;
	return input != NULL && out != NULL && (a <= b ? b - a < len : a - b < sizeof(*out));
}

ClusterControlRootResult
cluster_control_root_v2_history_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalHistoryImage *out)
{
	ClusterControlRootResult result;
	uint8 hash[32];
	uint32 count;
	bool valid_size = len >= 68 && len <= CLUSTER_WAL_HISTORY_MAX_BYTES;
	bool alias = history_overlaps_output(root, sizeof(*root), out)
				 || (valid_size && history_overlaps_output(bytes, len, out));

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || bytes == NULL || root == NULL || origin_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!valid_size)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (root->header.format_version != CONTROL_ROOT_HEADER_VERSION_V2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[origin_node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (root->refs[origin_node].history_generation == 0
		&& bytes_are_zero(root->refs[origin_node].history_sha256, 32))
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (root->refs[origin_node].history_generation == 0
		|| bytes_are_zero(root->refs[origin_node].history_sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (memcmp(bytes, "PGWH", 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(bytes + 4) != 1)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	count = read_u32_le(bytes + 8);
	if (read_u16_le(bytes + 6) != CLUSTER_WAL_HISTORY_HEADER_BYTES
		|| count > CLUSTER_WAL_HISTORY_MAX_RECORDS
		|| read_u32_le(bytes + 12) != CLUSTER_CONTROL_ROOT_RECORD_BYTES
		|| read_u64_le(bytes + 16) != (uint64)count * CLUSTER_CONTROL_ROOT_RECORD_BYTES
		|| len
			   != CLUSTER_WAL_HISTORY_HEADER_BYTES
					  + (size_t)count * CLUSTER_CONTROL_ROOT_RECORD_BYTES + 4)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (read_u64_le(bytes + 24) != root->header.system_identifier
		|| memcmp(bytes + 32, root->header.storage_uuid, 16) != 0
		|| memcmp(bytes + 48, root->header.authority_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (read_u32_le(bytes + len - 4) != control_root_crc(bytes, len - 4))
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	if (!control_root_sha256(bytes, len, hash))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, root->refs[origin_node].history_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;

	for (uint32 i = 0; i < count; i++) {
		const uint8 *src = bytes + CLUSTER_WAL_HISTORY_HEADER_BYTES
						   + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES;
		ClusterWalHistoryRecord *record = &out->records[i];
		uint64 incarnation;

		result
			= decode_record(src, origin_node + 1, &root->header, &record->snapshot,
							&record->record_crc32c, CONTROL_ROOT_RECORD_VERSION_V2, &record->refs);
		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			result = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto refused;
		incarnation = record->snapshot.identity.origin_owner_incarnation;
		if (incarnation == root->records[origin_node].identity.origin_owner_incarnation) {
			result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			goto refused;
		}
		/* A flat set preserves all retained writers without recursive traversal.
		 * Namespace aliases must not create two claims for one generation path.
		 * Numeric ordering here is canonical encoding only, never a redo order.
		 */
		if (record->refs.history_generation != 0 || !bytes_are_zero(record->refs.history_sha256, 32)
			|| (i > 0
				&& incarnation <= out->records[i - 1].snapshot.identity.origin_owner_incarnation)) {
			result = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
			goto refused;
		}
		record->publisher_incarnation = read_u64_le(src + 144);
		record->publisher_node = read_u32_le(src + 152);
	}
	out->count = count;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
refused:
	memset(out, 0, sizeof(*out));
	return result;
}

/* PGRAC: bounded encoder inputs cannot alias either caller-owned output.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static bool
history_ranges_overlap(const void *a, size_t na, const void *b, size_t nb)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return a != NULL && b != NULL && (x <= y ? y - x < na : x - y < nb);
}

ClusterControlRootResult
cluster_control_root_v2_history_encode(const ControlRootImage *root, uint32 origin_node,
									   const ClusterWalHistoryImage *history,
									   uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], size_t *length)
{
	ClusterControlRootResult result;
	size_t len;
	bool alias
		= history_ranges_overlap(root, sizeof(*root), bytes, CLUSTER_WAL_HISTORY_MAX_BYTES)
		  || history_ranges_overlap(history, sizeof(*history), bytes, CLUSTER_WAL_HISTORY_MAX_BYTES)
		  || history_ranges_overlap(root, sizeof(*root), length, sizeof(*length))
		  || history_ranges_overlap(history, sizeof(*history), length, sizeof(*length))
		  || history_ranges_overlap(bytes, CLUSTER_WAL_HISTORY_MAX_BYTES, length, sizeof(*length));

	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_WAL_HISTORY_MAX_BYTES);
	if (length != NULL)
		*length = 0;
	if (alias || root == NULL || history == NULL || bytes == NULL || length == NULL
		|| origin_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (root->header.format_version != CONTROL_ROOT_HEADER_VERSION_V2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[origin_node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (history->count > CLUSTER_WAL_HISTORY_MAX_RECORDS)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (root->records[origin_node].identity.origin_node_id != (int32)origin_node)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = snapshot_validate(&root->records[origin_node], origin_node + 1,
							   root->header.system_identifier, root->header.storage_uuid,
							   root->header.authority_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	for (uint32 i = 0; i < history->count; i++) {
		const ClusterWalHistoryRecord *r = &history->records[i];
		uint64 incarnation = r->snapshot.identity.origin_owner_incarnation;
		result = snapshot_validate(&r->snapshot, origin_node + 1, root->header.system_identifier,
								   root->header.storage_uuid, root->header.authority_uuid, true);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = record_refs_v2_validate(&r->refs);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (r->snapshot.identity.origin_node_id != (int32)origin_node
			|| incarnation == root->records[origin_node].identity.origin_owner_incarnation)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		if (r->refs.history_generation != 0 || !bytes_are_zero(r->refs.history_sha256, 32)
			|| (i > 0
				&& incarnation
					   <= history->records[i - 1].snapshot.identity.origin_owner_incarnation))
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (r->publisher_incarnation == 0 || r->publisher_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
			|| r->snapshot.lifecycle_reason < CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT
			|| r->snapshot.lifecycle_reason > CLUSTER_CONTROL_ROOT_PUBLISH_COPY_REPAIR)
			return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	}
	len = CLUSTER_WAL_HISTORY_HEADER_BYTES
		  + (size_t)history->count * CLUSTER_CONTROL_ROOT_RECORD_BYTES + 4;
	memcpy(bytes, "PGWH", 4);
	write_u16_le(bytes + 4, 1);
	write_u16_le(bytes + 6, CLUSTER_WAL_HISTORY_HEADER_BYTES);
	write_u32_le(bytes + 8, history->count);
	write_u32_le(bytes + 12, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u64_le(bytes + 16, (uint64)history->count * CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u64_le(bytes + 24, root->header.system_identifier);
	memcpy(bytes + 32, root->header.storage_uuid, 16);
	memcpy(bytes + 48, root->header.authority_uuid, 16);
	for (uint32 i = 0; i < history->count; i++) {
		const ClusterWalHistoryRecord *r = &history->records[i];
		encode_record_version(bytes + CLUSTER_WAL_HISTORY_HEADER_BYTES
								  + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
							  &r->snapshot, r->publisher_incarnation, r->publisher_node,
							  (ClusterControlRootPublishReason)r->snapshot.lifecycle_reason,
							  CONTROL_ROOT_RECORD_VERSION_V2, &r->refs);
	}
	write_u32_le(bytes + len - 4, control_root_crc(bytes, len - 4));
	*length = len;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
encode_header_version(ControlRootImage *image, uint16 version)
{
	uint8 *dst = image->bytes;

	memset(dst, 0, CLUSTER_CONTROL_ROOT_HEADER_BYTES);
	memcpy(dst, CONTROL_ROOT_HEADER_MAGIC, 4);
	write_u16_le(dst + 4, version);
	write_u16_le(dst + 6, CLUSTER_CONTROL_ROOT_HEADER_BYTES);
	write_u16_le(dst + 8, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u16_le(dst + 10, CLUSTER_CONTROL_ROOT_RECORD_COUNT);
	write_u32_le(dst + 12, CONTROL_ROOT_ENDIAN_TAG);
	write_u64_le(dst + 16, image->header.file_txn_seq);
	write_u64_le(dst + 24, image->header.system_identifier);
	memcpy(dst + 32, image->header.storage_uuid, 16);
	memcpy(dst + 48, image->header.authority_uuid, 16);
	write_u64_le(dst + 64, CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1);
	write_u16_le(dst + 72, version);
	write_u16_le(dst + 74, version);
	write_u32_le(dst + 76, image->header.activation_state);
	write_u64_le(dst + 80, (uint64)image->header.created_at_usec);
	write_u64_le(dst + 88, (uint64)image->header.published_at_usec);
	image->header.body_crc32c
		= control_root_crc(image->bytes + CONTROL_ROOT_BODY_OFFSET,
						   CLUSTER_CONTROL_ROOT_FILE_BYTES - CONTROL_ROOT_BODY_OFFSET);
	write_u32_le(dst + 96, image->header.body_crc32c);
	memcpy(dst + 100, image->header.migration_round_sha256, PG_SHA256_DIGEST_LENGTH);
	memcpy(dst + 132, image->header.source_wal_state_sha256, PG_SHA256_DIGEST_LENGTH);
	write_u64_le(dst + 164, image->header.migration_prepare_generation);
	write_u64_le(dst + 172, image->header.migration_transition_epoch);
	write_u64_le(dst + 180, image->header.source_feature_bitmap);
	write_u64_le(dst + 188, image->header.target_feature_bitmap);
	image->header.format_version = version;
	if (version == CONTROL_ROOT_HEADER_VERSION_V2) {
		const ControlRootCommonV2 *common = &image->header.v2;

		write_u32_le(dst + 196, common->database_state);
		write_u64_le(dst + 200, common->database_incarnation);
		write_u64_le(dst + 208, common->formation_seq);
		write_u64_le(dst + 216, common->configured[0]);
		write_u64_le(dst + 224, common->configured[1]);
		write_u64_le(dst + 232, common->serving[0]);
		write_u64_le(dst + 240, common->serving[1]);
		write_u64_le(dst + 248, common->config_generation);
		memcpy(dst + 256, common->config_sha256, 32);
		write_u64_le(dst + 288, common->control_image_generation);
		memcpy(dst + 296, common->control_image_sha256, 32);
		write_u64_le(dst + 328, common->catalog_manifest_generation);
		write_u64_le(dst + 336, common->global_scn_high_water);
		memcpy(dst + 344, common->catalog_manifest_sha256, 32);
	}
	image->header.header_crc32c = control_root_crc(dst, CONTROL_ROOT_HEADER_CRC_OFFSET);
	write_u32_le(dst + CONTROL_ROOT_HEADER_CRC_OFFSET, image->header.header_crc32c);
}

static void
encode_header(ControlRootImage *image)
{
	encode_header_version(image, CONTROL_ROOT_FORMAT_VERSION);
}

static ClusterControlRootResult
decode_image_version(ControlRootImage *image, const uint8 current_uuid[16], uint64 current_sysid,
					 uint16 expected_version)
{
	const uint8 *src = image->bytes;
	uint32 stored_crc;
	uint16 i;

	memset(&image->header, 0, sizeof(image->header));
	memset(image->records, 0, sizeof(image->records));
	memset(image->refs, 0, sizeof(image->refs));
	memset(image->publisher_incarnation, 0, sizeof(image->publisher_incarnation));
	memset(image->publisher_node, 0, sizeof(image->publisher_node));
	memset(image->record_crc32c, 0, sizeof(image->record_crc32c));
	memset(image->present, 0, sizeof(image->present));
	if (memcmp(src, CONTROL_ROOT_HEADER_MAGIC, 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(src + 4) != expected_version
		|| read_u16_le(src + 6) != CLUSTER_CONTROL_ROOT_HEADER_BYTES
		|| read_u16_le(src + 8) != CLUSTER_CONTROL_ROOT_RECORD_BYTES
		|| read_u16_le(src + 10) != CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| read_u16_le(src + 72) != expected_version || read_u16_le(src + 74) != expected_version
		|| read_u64_le(src + 64) != CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (read_u32_le(src + 12) != CONTROL_ROOT_ENDIAN_TAG)
		return CLUSTER_CONTROL_ROOT_BAD_ENDIAN;
	stored_crc = read_u32_le(src + CONTROL_ROOT_HEADER_CRC_OFFSET);
	if (stored_crc != control_root_crc(src, CONTROL_ROOT_HEADER_CRC_OFFSET))
		return CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC;
	if (!bytes_are_zero(src + (expected_version == CONTROL_ROOT_HEADER_VERSION_V2 ? 376 : 196),
						expected_version == CONTROL_ROOT_HEADER_VERSION_V2 ? 128 : 308)
		|| !bytes_are_zero(src + 508, 4))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	image->header.file_txn_seq = read_u64_le(src + 16);
	image->header.system_identifier = read_u64_le(src + 24);
	memcpy(image->header.storage_uuid, src + 32, 16);
	memcpy(image->header.authority_uuid, src + 48, 16);
	image->header.activation_state = read_u32_le(src + 76);
	image->header.created_at_usec = (int64)read_u64_le(src + 80);
	image->header.published_at_usec = (int64)read_u64_le(src + 88);
	image->header.body_crc32c = read_u32_le(src + 96);
	memcpy(image->header.migration_round_sha256, src + 100, PG_SHA256_DIGEST_LENGTH);
	memcpy(image->header.source_wal_state_sha256, src + 132, PG_SHA256_DIGEST_LENGTH);
	image->header.migration_prepare_generation = read_u64_le(src + 164);
	image->header.migration_transition_epoch = read_u64_le(src + 172);
	image->header.source_feature_bitmap = read_u64_le(src + 180);
	image->header.target_feature_bitmap = read_u64_le(src + 188);
	image->header.header_crc32c = stored_crc;
	image->header.format_version = expected_version;
	if (image->header.file_txn_seq == 0 || image->header.system_identifier == 0
		|| image->header.system_identifier != current_sysid
		|| memcmp(image->header.storage_uuid, current_uuid, 16) != 0
		|| !uuid_v4_valid(image->header.authority_uuid))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (image->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED
		&& image->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (bytes_are_zero(image->header.migration_round_sha256, PG_SHA256_DIGEST_LENGTH)
		|| bytes_are_zero(image->header.source_wal_state_sha256, PG_SHA256_DIGEST_LENGTH)
		|| image->header.migration_prepare_generation == 0
		|| !cluster_control_root_feature_bitmap_is_known(image->header.source_feature_bitmap)
		|| !cluster_control_root_feature_bitmap_is_known(image->header.target_feature_bitmap)
		|| (image->header.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1)
			   == 0)
		return CLUSTER_CONTROL_ROOT_MIXED_VERSION;
	if (expected_version == CONTROL_ROOT_HEADER_VERSION_V2) {
		ControlRootCommonV2 *common = &image->header.v2;
		ClusterControlRootResult result;

		common->database_state = read_u32_le(src + 196);
		common->database_incarnation = read_u64_le(src + 200);
		common->formation_seq = read_u64_le(src + 208);
		common->configured[0] = read_u64_le(src + 216);
		common->configured[1] = read_u64_le(src + 224);
		common->serving[0] = read_u64_le(src + 232);
		common->serving[1] = read_u64_le(src + 240);
		common->config_generation = read_u64_le(src + 248);
		memcpy(common->config_sha256, src + 256, 32);
		common->control_image_generation = read_u64_le(src + 288);
		memcpy(common->control_image_sha256, src + 296, 32);
		common->catalog_manifest_generation = read_u64_le(src + 328);
		common->global_scn_high_water = read_u64_le(src + 336);
		memcpy(common->catalog_manifest_sha256, src + 344, 32);
		result = common_v2_validate(common);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (image->header.body_crc32c
		!= control_root_crc(src + CONTROL_ROOT_BODY_OFFSET,
							CLUSTER_CONTROL_ROOT_FILE_BYTES - CONTROL_ROOT_BODY_OFFSET))
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;

	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
		const uint8 *record = src + CLUSTER_CONTROL_ROOT_HEADER_BYTES
							  + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES;
		ClusterControlRootResult result = decode_record(
			record, (uint16)(i + 1), &image->header, &image->records[i], &image->record_crc32c[i],
			expected_version == CONTROL_ROOT_HEADER_VERSION_V2 ? CONTROL_ROOT_RECORD_VERSION_V2
															   : CONTROL_ROOT_FORMAT_VERSION,
			&image->refs[i]);

		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			continue;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		image->present[i] = true;
		image->publisher_incarnation[i] = read_u64_le(record + 144);
		image->publisher_node[i] = read_u32_le(record + 152);
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: these codecs validate representation, not proof of publication.
 * Existing v1 file APIs never call them. Author: SqlRush <sqlrush@gmail.com>
 */
ClusterControlRootResult
cluster_control_root_v2_decode(const uint8 *bytes, size_t len, const uint8 storage_uuid[16],
							   uint64 system_identifier, ControlRootImage *out)
{
	ClusterControlRootResult result;
	uint8 expected_uuid[16];

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (bytes == NULL || storage_uuid == NULL || system_identifier == 0
		|| bytes_are_zero(storage_uuid, 16)) {
		memset(out, 0, sizeof(*out));
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	}
	if (len != CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		memset(out, 0, sizeof(*out));
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	}
	memcpy(expected_uuid, storage_uuid, sizeof(expected_uuid));
	memmove(out->bytes, bytes, len);
	result = decode_image_version(out, expected_uuid, system_identifier,
								  CONTROL_ROOT_HEADER_VERSION_V2);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

ClusterControlRootResult
cluster_control_root_v2_encode(ControlRootImage *image)
{
	ClusterControlRootResult result;
	uint8 expected_uuid[16];
	uint64 system_identifier;
	uint16 i;

	if (image == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(image->bytes, 0, sizeof(image->bytes));
	if (image->header.format_version != CONTROL_ROOT_HEADER_VERSION_V2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (bytes_are_zero(image->header.storage_uuid, 16))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = common_v2_validate(&image->header.v2);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++i) {
		if (!image->present[i]) {
			if (!bytes_are_zero(&image->records[i], sizeof(image->records[i]))
				|| !bytes_are_zero(&image->refs[i], sizeof(image->refs[i]))
				|| image->publisher_incarnation[i] != 0 || image->publisher_node[i] != 0)
				return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
			continue;
		}
		result = snapshot_validate(&image->records[i], i + 1, image->header.system_identifier,
								   image->header.storage_uuid, image->header.authority_uuid, true);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (image->records[i].identity.origin_node_id != i)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		if (image->publisher_incarnation[i] == 0 || image->publisher_node[i] >= 128)
			return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
		result = record_refs_v2_validate(&image->refs[i]);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++i)
		if (image->present[i])
			encode_record_version(
				image->bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES
					+ (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
				&image->records[i], image->publisher_incarnation[i], image->publisher_node[i],
				(ClusterControlRootPublishReason)image->records[i].lifecycle_reason,
				CONTROL_ROOT_RECORD_VERSION_V2, &image->refs[i]);
	encode_header_version(image, CONTROL_ROOT_HEADER_VERSION_V2);
	memcpy(expected_uuid, image->header.storage_uuid, sizeof(expected_uuid));
	system_identifier = image->header.system_identifier;
	result = decode_image_version(image, expected_uuid, system_identifier,
								  CONTROL_ROOT_HEADER_VERSION_V2);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(image->bytes, 0, sizeof(image->bytes));
	return result;
}

static ClusterControlRootResult
read_exact_file(const char *path, uint8 *bytes)
{
	struct stat st;
	size_t done = 0;
	int fd;

	if (!regular_or_absent_nosymlink(path, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (fstat(fd, &st) != 0) {
		CloseTransientFile(fd);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	if (st.st_size != CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		CloseTransientFile(fd);
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	}
	while (done < CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		ssize_t n = read(fd, bytes + done, CLUSTER_CONTROL_ROOT_FILE_BYTES - done);

		if (n <= 0) {
			CloseTransientFile(fd);
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		done += (size_t)n;
	}
	if (CloseTransientFile(fd) != 0)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
read_one_image_version(const char *path, const uint8 current_uuid[16], uint64 current_sysid,
					   ControlRootImage *image, uint16 version)
{
	ClusterControlRootResult result;

	result = read_exact_file(path, image->bytes);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return decode_image_version(image, current_uuid, current_sysid, version);
}

static ClusterControlRootResult
read_one_image(const char *path, const uint8 current_uuid[16], uint64 current_sysid,
			   ControlRootImage *image)
{
	return read_one_image_version(path, current_uuid, current_sysid, image,
								  CONTROL_ROOT_FORMAT_VERSION);
}

static bool
same_immutable_header(const ControlRootImage *left, const ControlRootImage *right)
{
	return left->header.system_identifier == right->header.system_identifier
		   && memcmp(left->header.storage_uuid, right->header.storage_uuid, 16) == 0
		   && memcmp(left->header.authority_uuid, right->header.authority_uuid, 16) == 0
		   && memcmp(left->header.migration_round_sha256, right->header.migration_round_sha256,
					 PG_SHA256_DIGEST_LENGTH)
				  == 0
		   && memcmp(left->header.source_wal_state_sha256, right->header.source_wal_state_sha256,
					 PG_SHA256_DIGEST_LENGTH)
				  == 0
		   && left->header.migration_prepare_generation
				  == right->header.migration_prepare_generation
		   && left->header.migration_transition_epoch == right->header.migration_transition_epoch
		   && left->header.source_feature_bitmap == right->header.source_feature_bitmap
		   && left->header.target_feature_bitmap == right->header.target_feature_bitmap;
}

static ClusterControlRootResult
read_canonical_pair_version(ControlRootImage *primary, ControlRootImage *bak,
							const uint8 current_uuid[16], uint64 current_sysid, uint16 version)
{
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];
	ClusterControlRootResult primary_result;
	ClusterControlRootResult bak_result;

	if (!build_control_path(primary_path, sizeof(primary_path), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak_path, sizeof(bak_path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	primary_result
		= read_one_image_version(primary_path, current_uuid, current_sysid, primary, version);
	bak_result = read_one_image_version(bak_path, current_uuid, current_sysid, bak, version);
	if (primary_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (bak_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (!same_immutable_header(primary, bak)
				|| bak->header.file_txn_seq > primary->header.file_txn_seq
				|| (bak->header.file_txn_seq == primary->header.file_txn_seq
					&& memcmp(primary->bytes, bak->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0))
				return CLUSTER_CONTROL_ROOT_COPY_DIVERGENT;
			return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
		}
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED;
	}
	if (bak_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED;
	if (primary_result != CLUSTER_CONTROL_ROOT_ABSENT)
		return primary_result;
	return bak_result;
}

static ClusterControlRootResult
read_canonical_pair(ControlRootImage *primary, ControlRootImage *bak)
{
	uint8 current_uuid[16];
	uint64 current_sysid;

	if (!current_storage_uuid(current_uuid))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	current_sysid = GetSystemIdentifier();
	if (current_sysid == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	return read_canonical_pair_version(primary, bak, current_uuid, current_sysid,
									   CONTROL_ROOT_FORMAT_VERSION);
}

/* PGRAC: read the exact common image selected by a primary v2 root. Holding
 * CF-S/X prevents an installer/GC from replacing the root or unlinking its
 * selected object during this read. No nested lock, projection or bak fallback.
 * This is deliberately NOT a per-thread startup view or admission decision.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterControlRootResult
cluster_control_root_v2_read_control_locked(const uint8 storage_uuid[16], uint64 system_identifier,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token)
{
	ControlRootImage *bak;
	ClusterControlRootResult result;
	ClusterControlRootResult root_result;

	if (root != NULL)
		memset(root, 0, sizeof(*root));
	if (common != NULL)
		memset(common, 0, sizeof(*common));
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (root == NULL || common == NULL || token == NULL || storage_uuid == NULL
		|| bytes_are_zero(storage_uuid, 16) || system_identifier == 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ShareLock)
		&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	/* Never run a write probe while holding CF. The enclosing adapter must
	 * already have qualified the storage contract and revalidate its writer
	 * authority separately if it intends to publish anything.
	 */
	result = storage_contract_check(storage_uuid, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* A single configured/surviving instance does not turn shared v2 storage
	 * into the legacy local-file profile. Qualification remains mandatory.
	 */
	if (cluster_cf_contract_load(DataDir) != CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED)
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	bak = palloc(sizeof(*bak));
	root_result = read_canonical_pair_version(root, bak, storage_uuid, system_identifier,
											  CONTROL_ROOT_HEADER_VERSION_V2);
	pfree(bak);
	result = root_result;
	if (root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| root_result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		result = cluster_cf_control_image_read_locked(root->header.v2.control_image_generation,
													  root->header.v2.control_image_sha256,
													  system_identifier, common);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			ClusterSharedConfigRef ref;
			ClusterSharedConfigImage config;

			/* PGRAC: a generation/hash without its exact configuration object
			 * is not a complete root view. This checks binding/representation,
			 * not GUC application or permission to serve.
			 * Author: SqlRush <sqlrush@gmail.com>
			 */
			memset(&ref, 0, sizeof(ref));
			ref.identity.system_identifier = system_identifier;
			ref.identity.database_incarnation = root->header.v2.database_incarnation;
			ref.identity.generation = root->header.v2.config_generation;
			memcpy(ref.identity.storage_uuid, root->header.storage_uuid, 16);
			memcpy(ref.identity.authority_uuid, root->header.authority_uuid, 16);
			memcpy(ref.identity.configured, root->header.v2.configured,
				   sizeof(ref.identity.configured));
			memcpy(ref.sha256, root->header.v2.config_sha256, 32);
			result = cluster_shared_config_read_locked(cluster_shared_data_dir, &ref, &config);
			cluster_shared_config_free(&config);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			make_file_token(root, token);
			if (token->file_txn_seq == 0)
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			else
				return root_result;
		}
	}
	/* A valid root alone must not escape after its selected image failed. */
	memset(root, 0, sizeof(*root));
	memset(common, 0, sizeof(*common));
	memset(token, 0, sizeof(*token));
	return result;
}

/* PGRAC: root-selected common fields plus this exact thread's restart inputs.
 * All objects are consumed under the caller's existing CF-S/X; no nested CF
 * or compatibility fallback. The returned root remains necessary for the
 * subsequent lifecycle/config/admission checks, which this read cannot grant.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterControlRootResult
cluster_control_root_v2_read_thread_locked(const ClusterControlRootIdentity *self,
										   ControlRootImage *root, ControlFileData *out,
										   ClusterControlRootFileToken *token)
{
	ControlFileData common;
	ClusterRecoveryAnchorRefV2 ref;
	ClusterWalThreadClaimRefV2 claim_ref;
	ClusterWalThreadClaimV2 claim;
	ClusterControlRootResult result, root_result;
	int index;

	if (root != NULL)
		memset(root, 0, sizeof(*root));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (self == NULL || root == NULL || out == NULL || token == NULL || self->origin_thread_id == 0
		|| self->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	root_result = cluster_control_root_v2_read_control_locked(
		self->storage_uuid, self->system_identifier, root, &common, token);
	result = root_result;
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		goto fail;
	index = self->origin_thread_id - 1;
	if (!root->present[index]) {
		result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto fail;
	}
	if (!cluster_control_root_identity_equal(self, &root->records[index].identity)) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto fail;
	}
	/* PGRAC: neither the directory name nor an anchor substitutes for the
	 * physical root-selected generation claim. Keep the existing CF interval.
	 * Author: SqlRush <sqlrush@gmail.com>
	 */
	memset(&claim_ref, 0, sizeof(claim_ref));
	claim_ref.identity = root->records[index].identity;
	claim_ref.database_incarnation = root->header.v2.database_incarnation;
	claim_ref.max_config_generation = root->header.v2.config_generation;
	memcpy(claim_ref.claim_sha256, root->refs[index].claim_sha256, 32);
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &claim_ref, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto fail;
	memset(&ref, 0, sizeof(ref));
	ref.identity = root->records[index].identity;
	ref.database_incarnation = root->header.v2.database_incarnation;
	ref.max_config_generation = root->header.v2.config_generation;
	ref.anchor_generation = root->refs[index].anchor_generation;
	memcpy(ref.anchor_sha256, root->refs[index].anchor_sha256, 32);
	memcpy(ref.claim_sha256, root->refs[index].claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_locked(&ref, &common, out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto fail;
	result = cluster_recovery_anchor_v2_thread_state(&ref, &root->records[index], out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto fail;
	return root_result;
fail:
	memset(root, 0, sizeof(*root));
	memset(out, 0, sizeof(*out));
	memset(token, 0, sizeof(*token));
	return result;
}

/* PGRAC: a read-only runtime view requires a live exact local owner; this is
 * not the early postmaster sizing read or the recovery owner's read API.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static bool
runtime_v2_owner_current(uint64 epoch, uint64 incarnation)
{
	return cluster_shared_config && cluster_enabled && cluster_controlfile_shared_authority
		   && cluster_node_id >= 0 && cluster_node_id < CLUSTER_MAX_NODES && epoch != 0
		   && incarnation != 0 && cluster_qvotec_get_self_incarnation() == incarnation
		   && cluster_membership_get_state(cluster_node_id) == CLUSTER_MEMBER_MEMBER
		   && cluster_membership_get_last_admitted_incarnation(cluster_node_id) == incarnation
		   && cluster_wal_thread_dir_validated()
		   && cluster_wal_thread_id() == (uint16)(cluster_node_id + 1)
		   && !cluster_reconfig_has_pending_prebump_stage() && cluster_serving_ready_is_current()
		   && cluster_write_fence_allowed() && cluster_epoch_get_current() == epoch;
}

ClusterControlRootResult
cluster_control_root_v2_read_runtime_local_locked(ControlFileData *out)
{
	ControlRootImage *root;
	ControlFileData common, thread;
	ClusterControlRootIdentity self;
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;
	uint8 storage_uuid[16];
	uint64 epoch, incarnation, sysid;
	int node;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!cluster_shared_config || !cluster_enabled || !cluster_controlfile_shared_authority
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ShareLock)
		&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	node = cluster_node_id;
	epoch = cluster_epoch_get_current();
	incarnation = cluster_qvotec_get_self_incarnation();
	if (!runtime_v2_owner_current(epoch, incarnation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!current_storage_uuid(storage_uuid))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	sysid = GetSystemIdentifier();
	if (sysid == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	root = palloc(sizeof(*root));
	result
		= cluster_control_root_v2_read_control_locked(storage_uuid, sysid, root, &common, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		goto done;
	if (!root->present[node]
		|| root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		|| root->records[node].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| (root->header.v2.serving[node / 64] & (UINT64_C(1) << (node % 64))) == 0) {
		result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
		goto done;
	}
	self = root->records[node].identity;
	if (self.origin_owner_incarnation != incarnation) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	/* The same externally held CF interval pins the selected root across both
	 * reads. The second composes the exact physical claim and thread anchor.
	 */
	result = cluster_control_root_v2_read_thread_locked(&self, root, &thread, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		goto done;
	if (!runtime_v2_owner_current(epoch, incarnation)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	*out = thread;
done:
	pfree(root);
	return result;
}

/*
 * cluster_control_root_restore_bit22_latch_if_active -- RF-ROOT P9 verification
 *	(contract): re-arm the bit22 cutover latch across a postmaster restart.
 *	The shmem latch lives only as long as the postmaster; a durable ACTIVE
 *	root whose target bitmap carries bit22 means the cutover round
 *	completed and the dual-path gate (§17.8) must read as post-bit22 on
 *	the next boot.  The round identity is re-bound from the root header
 *	(migration_transition_epoch / migration_prepare_generation) through
 *	the same 0->1 CAS the in-round apply uses — a concurrent winner or an
 *	already-armed latch is a no-op, and a census-RED apply fails closed
 *	(the gate then stays pre-bit22, the registry path: safe direction).
 *	Returns whether the gate reads as post-bit22 afterwards.
 */
bool
cluster_control_root_restore_bit22_latch_if_active(void)
{
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES];
	ClusterSemanticActivationRecord open;
	ControlRootImage primary;
	ControlRootImage bak;
	ClusterSemanticActivationResult qv_result;
	ClusterControlRootResult root_result;
	bool implicit_open = false;

	if (cluster_r4_bit22_cutover_active())
		return true;
	/* RF-ROOT P9 verification (verified implementation): the latch restores only on
	 * the DURABLE Target OPEN proof — a strict-majority OPEN(P+2) record
	 * on the voting disks (the cutover round's own final record), cross-
	 * matched to the ACTIVE canonical root's round identity (root
	 * migration_transition_epoch == OPEN.transition_epoch AND root
	 * migration_prepare_generation + 2 == OPEN.record_generation).
	 * Neither the root ACTIVE state alone (the all-member OPEN_APPLIED
	 * may not have completed) nor any record lifecycle axis is used.
	 * The apply lands at TARGET_BOOTSTRAP (recovery planning may select
	 * TARGET_VERIFIED before ordinary serving.
	 * R4 cutover contract (verified implementation): `implicit_open` only reports
	 * whether the majority-selected image is the all-zero pre-R4 sentinel
	 * — a REAL durable OPEN(P+2) record reads back nonzero, so the flag
	 * must NOT gate the restore (a post-bit22 restart would never re-arm
	 * the latch and the control-plane gates would stay closed). */
	qv_result = cluster_qvotec_bootstrap_read_semantic_activation(selected, &implicit_open);
	(void)implicit_open;
	if (qv_result != CLUSTER_SEMANTIC_ACTIVATION_OK
		|| !cluster_semantic_activation_record_decode(selected, &open, NULL)
		|| open.phase != CLUSTER_SEMANTIC_PHASE_OPEN)
		return false;
	root_result = read_canonical_pair(&primary, &bak);
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return false;
	if (primary.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| (primary.header.target_feature_bitmap
			& PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1)
			   == 0
		|| primary.header.migration_transition_epoch != open.transition_epoch
		|| primary.header.migration_prepare_generation == UINT64_MAX
		|| primary.header.migration_prepare_generation + 2 != open.record_generation)
		return false;
	return cluster_r4_bit22_cutover_latch_apply(open.transition_epoch, open.record_generation);
}

/*
 * cluster_control_root_bootstrap_validate_active_round -- RF-ROOT P9 verification
 *	#2 closure (verified implementation): startup/member-side verification that the
 *	canonical root is ACTIVE and bound to exactly this cutover round
 *	(migration_round_sha256 == round_sha256(round)).  Full canonical
 *	validation (storage uuid / sysid / header+body CRC / primary-bak
 *	coherence) via read_canonical_pair.  Forms a read-only proof: it
 *	grants no token authority beyond the returned file token.
 */
ClusterControlRootResult
cluster_control_root_bootstrap_validate_active_round(
	const ClusterControlRootMigrationRoundV1 *round, ClusterControlRootFileToken *token)
{
	ControlRootImage primary;
	ControlRootImage bak;
	uint8 sha[PG_SHA256_DIGEST_LENGTH];
	ClusterControlRootResult result;

	if (round == NULL || !cluster_control_root_round_sha256(round, sha))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = read_canonical_pair(&primary, &bak);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (primary.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| memcmp(primary.header.migration_round_sha256, sha, PG_SHA256_DIGEST_LENGTH) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (token != NULL)
		make_file_token(&primary, token);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/*
 * cluster_control_root_bootstrap_validate_active_round_fields -- RF-ROOT
 * P9 verification (cold-formation): member-side twin of
 * bootstrap_validate_active_round for the bit22 cutover round.  The full
 * round (and its sha) is coordinator-local (the seam lives in coordinator
 * shmem, which other NODES cannot see), so a member cannot recompute
 * round_sha256.  It binds the ACTIVE root to the round identity it DOES
 * hold — the ACK table's transition_epoch / prepare_generation (= table
 * generation - 1) / source+target feature bitmaps — plus the invariant
 * that the root carries a non-zero round sha (the coordinator wrote it
 * under the create/activate proofs; header+body CRCs and the primary/bak
 * coherence already validated).  Full canonical validation via
 * read_canonical_pair; read-only, grants no token authority.
 */
ClusterControlRootResult
cluster_control_root_bootstrap_validate_active_round_fields(uint64 transition_epoch,
															uint64 prepare_generation,
															uint64 source_feature_bitmap,
															uint64 target_feature_bitmap)
{
	ControlRootImage primary;
	ControlRootImage bak;
	ClusterControlRootResult result;

	result = read_canonical_pair(&primary, &bak);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (primary.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| primary.header.migration_transition_epoch != transition_epoch
		|| primary.header.migration_prepare_generation != prepare_generation
		|| primary.header.source_feature_bitmap != source_feature_bitmap
		|| primary.header.target_feature_bitmap != target_feature_bitmap
		|| bytes_are_zero(primary.header.migration_round_sha256,
						  sizeof(primary.header.migration_round_sha256)))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
make_read_token(const ControlRootImage *image, uint16 thread_id, uint8 source,
				ClusterControlRootReadToken *token)
{
	const ClusterControlRootSnapshot *snapshot = &image->records[thread_id - 1];

	memset(token, 0, sizeof(*token));
	memcpy(token->authority_uuid, snapshot->identity.authority_uuid, 16);
	token->origin_thread_id = thread_id;
	token->source = source;
	token->lifecycle = (uint8)snapshot->lifecycle;
	token->root_lineage_seq = snapshot->identity.root_lineage_seq;
	token->file_txn_seq = image->header.file_txn_seq;
	token->root_publish_seq = snapshot->root_publish_seq;
	token->record_crc32c = image->record_crc32c[thread_id - 1];
	token->root_flags = snapshot->root_flags;
}

static bool
read_token_equal(const ClusterControlRootReadToken *left, const ClusterControlRootReadToken *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

static bool
acquire_clusterwide_cf(LOCKMODE mode)
{
	if (!cluster_cf_lock(mode))
		return false;
	if (!cluster_cf_held_is_clusterwide(mode)) {
		(void)cluster_cf_unlock_confirmed(mode);
		return false;
	}
	return true;
}

static ClusterControlRootResult
release_cf(LOCKMODE mode, ClusterControlRootResult result)
{
	if (cluster_cf_unlock_confirmed(mode) != CLUSTER_CF_RELEASE_CONFIRMED)
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	return result;
}

/* PGRAC: a normal checkpoint may reclaim only its exact live writer's
 * generation. Never upgrade this purpose into a recovery/history read.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterControlRootResult
cluster_control_root_v2_read_retention_current(const ClusterControlRootIdentity *self,
											   ClusterControlRootSnapshot *out,
											   ClusterControlRootReadToken *token)
{
	ControlRootImage *root;
	ControlFileData thread;
	ClusterControlRootIdentity expected;
	ClusterControlRootFileToken file_token;
	ClusterControlRootReadToken selected;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	volatile bool held = false;
	uint64 epoch, incarnation;
	uint32 index;

	if (self != NULL)
		expected = *self;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (self == NULL || out == NULL || token == NULL || !cluster_shared_config || !cluster_enabled
		|| !cluster_controlfile_shared_authority || expected.origin_node_id != cluster_node_id
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| expected.origin_thread_id != cluster_node_id + 1
		|| expected.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	epoch = cluster_epoch_get_current();
	incarnation = cluster_qvotec_get_self_incarnation();
	if (!runtime_v2_owner_current(epoch, incarnation)
		|| expected.origin_owner_incarnation != incarnation)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	index = expected.origin_thread_id - 1;
	root = palloc(sizeof(*root));
	PG_TRY();
	{
		if (cluster_cf_lock(ShareLock)) {
			held = true;
			if (cluster_cf_held_is_clusterwide(ShareLock)) {
				result = cluster_control_root_v2_read_thread_locked(&expected, root, &thread,
																	&file_token);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
					if (root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
						|| root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
						|| root->records[index].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
						|| (root->header.v2.serving[index / 64] & (UINT64_C(1) << (index % 64)))
							   == 0)
						result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
					else if (!runtime_v2_owner_current(epoch, incarnation))
						result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
					else
						make_read_token(root, expected.origin_thread_id,
										CONTROL_ROOT_SOURCE_PRIMARY, &selected);
				}
			}
			result = release_cf(ShareLock, result);
			held = false;
		}
	}
	PG_CATCH();
	{
		ClusterControlRootResult cleanup = CLUSTER_CONTROL_ROOT_IO_ERROR;

		if (held)
			cleanup = release_cf(ShareLock, cleanup);
		pfree(root);
		if (cleanup == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm control-root retention read-lock cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		*out = root->records[index];
		*token = selected;
	}
	pfree(root);
	/* E1 normally treats unavailable authority as retain-and-continue. An
	 * unconfirmed CF release is not such a refusal: terminate this owner
	 * instead of leaving a hidden hold that blocks every later checkpoint.
	 * Keep this at the actual reader so no bool/guard adapter can erase it. */
	if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
		elog(FATAL, "could not confirm control-root retention read-lock release");
	return result;
}

/* PGRAC: recovery owners may inspect a failed peer, not only their own live
 * writer. Select and authenticate the v2 root/config/claim/anchor under one
 * owned CF-S interval. This mints an observation, never serving permission,
 * isolation proof, a retention pin or permission to finish recovery.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
read_canonical_v2(uint16 thread_id, const ClusterControlRootIdentity *expected_identity,
				  ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token)
{
	ControlRootImage *root;
	ControlFileData thread;
	ClusterControlRootIdentity expected;
	ClusterControlRootFileToken discovered, file_token;
	ClusterControlRootReadToken selected;
	ClusterControlRootResult result;
	volatile bool held = false;
	uint8 storage_uuid[16];
	uint64 system_identifier;
	uint32 index;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (!cluster_shared_config || !cluster_enabled || !cluster_controlfile_shared_authority
		|| thread_id == 0 || thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!current_storage_uuid(storage_uuid))
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	system_identifier = GetSystemIdentifier();
	if (system_identifier == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (expected_identity != NULL) {
		expected = *expected_identity;
		if (expected.origin_thread_id != thread_id
			|| expected.system_identifier != system_identifier
			|| memcmp(expected.storage_uuid, storage_uuid, 16) != 0)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	}
	result = storage_contract_check(storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	index = thread_id - 1;
	root = palloc(sizeof(*root));
	result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	PG_TRY();
	{
		if (cluster_cf_lock(ShareLock)) {
			held = true;
			if (cluster_cf_held_is_clusterwide(ShareLock)) {
				if (expected_identity == NULL) {
					result = cluster_control_root_v2_read_control_locked(
						storage_uuid, system_identifier, root, &thread, &discovered);
					if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
						|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
						if (!root->present[index])
							result = CLUSTER_CONTROL_ROOT_ABSENT;
						else
							expected = root->records[index].identity;
					}
				} else
					result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
					result = cluster_control_root_v2_read_thread_locked(&expected, root, &thread,
																		&file_token);
					if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
						|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
						if (expected_identity == NULL
							&& memcmp(&discovered, &file_token, sizeof(file_token)) != 0)
							result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
						else
							make_read_token(root, thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
											&selected);
					}
				}
			}
			result = release_cf(ShareLock, result);
			held = false;
		}
	}
	PG_CATCH();
	{
		ClusterControlRootResult cleanup = CLUSTER_CONTROL_ROOT_IO_ERROR;

		if (held)
			cleanup = release_cf(ShareLock, cleanup);
		pfree(root);
		if (cleanup == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm canonical control-root read-lock cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (out != NULL)
			*out = root->records[index];
		if (token != NULL)
			*token = selected;
	}
	pfree(root);
	if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
		elog(FATAL, "could not confirm canonical control-root read-lock release");
	return result;
}

ClusterControlRootResult
cluster_control_root_read_canonical(uint16 origin_thread_id,
									const ClusterControlRootIdentity *expected_identity,
									ClusterControlRootReadMode mode,
									ClusterControlRootSnapshot *out_snapshot,
									ClusterControlRootReadToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult result;
	uint8 expected_storage[16];
	bool strong;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	strong = mode == CLUSTER_CONTROL_ROOT_READ_STRONG;
	if (origin_thread_id == 0 || origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| (mode != CLUSTER_CONTROL_ROOT_READ_STRONG
			&& mode != CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE)
		|| (strong && expected_identity == NULL))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_shared_config) {
		if (!strong)
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		return read_canonical_v2(origin_thread_id, expected_identity, out_snapshot, out_token);
	}
	if (expected_identity != NULL)
		memcpy(expected_storage, expected_identity->storage_uuid, 16);
	result = storage_contract_check(expected_identity != NULL ? expected_storage : NULL, strong);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (strong && !acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;

	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& !primary->present[origin_thread_id - 1])
		result = CLUSTER_CONTROL_ROOT_ABSENT;
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& expected_identity != NULL
		&& !cluster_control_root_identity_equal(expected_identity,
												&primary->records[origin_thread_id - 1].identity))
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		snapshot = primary->records[origin_thread_id - 1];
		memset(&token, 0, sizeof(token));
		if (strong) {
			make_read_token(primary, origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &token);
			/* RF-ROOT P9 verification (implementation): a STRONG read is the CF(S)-
			 * bound phase-4 revalidation — upgrade a bootstrapped bit22
			 * latch to TARGET_VERIFIED (the serving/admission gate).
			 * Idempotent; a SOURCE latch is left untouched. */
			(void)cluster_r4_bit22_cutover_latch_verify();
		}
	}
	pfree(bak);
	pfree(primary);
	if (strong)
		result = release_cf(ShareLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (out_snapshot != NULL)
			*out_snapshot = snapshot;
		if (strong && out_token != NULL)
			*out_token = token;
	}
	return result;
}

/*
 * cluster_control_root_read_canonical_discovered -- RF-ROOT P7 (Stage 8 contract
 *	contract §A, contract follow-up): the legal no-prior-identity STRONG read.
 *	A STRONG read requires a bound expected_identity (control_root.c arg
 *	check: STRONG+NULL -> INVALID_ARGUMENT), so a caller that does not yet
 *	know the record's identity performs the committed two-step pattern
 *	(wal_retention.c precedent): BOOTSTRAP_VALIDATE discovers the identity
 *	(validation semantics only — no CF hold, no token minted), then the
 *	STRONG read binds that exact identity and mints the token.  A republish
 *	between the steps lands IDENTITY_MISMATCH (fail-closed; the caller's
 *	next pass retries).  BOOTSTRAP output never serves a correctness
 *	decision directly.
 */
ClusterControlRootResult
cluster_control_root_read_canonical_discovered(uint16 origin_thread_id,
											   ClusterControlRootSnapshot *out_snapshot,
											   ClusterControlRootReadToken *out_token)
{
	ClusterControlRootSnapshot bootstrap;
	ClusterControlRootIdentity discovered;
	ClusterControlRootResult result;

	/* Clear the caller's outputs up front: a failed discovery must never
	 * leak stale caller data (fail-closed hygiene, mirrors read_canonical). */
	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	/* PRE2 discovery is inside the same owned CF interval as the exact
	 * object reads, never a legacy lock-free bootstrap permission. */
	if (cluster_shared_config)
		return read_canonical_v2(origin_thread_id, NULL, out_snapshot, out_token);
	memset(&bootstrap, 0, sizeof(bootstrap));
	result = cluster_control_root_read_canonical(
		origin_thread_id, NULL, CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE, &bootstrap, NULL);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	discovered = bootstrap.identity;
	return cluster_control_root_read_canonical(
		origin_thread_id, &discovered, CLUSTER_CONTROL_ROOT_READ_STRONG, out_snapshot, out_token);
}

/*
 * cluster_control_root_read_canonical_dead_origin -- Stage 8 contract
 *	(verified implementation): lock-free canonical read for a DEAD origin's thread.
 *
 *	The STRONG read's clusterwide CF share lock guards against a republish
 *	racing the identity between discovery and bind.  A dead origin has no
 *	live writer (its only publisher was the dead postmaster) and the GRD
 *	recovery episode adopts each shard to exactly one survivor, so no
 *	concurrent republish exists: the lock-free read is the final value.
 *	"BOOTSTRAP output never serves a correctness decision" targets live
 *	contested reads; here the data is static.  Used by the post-bit22
 *	hw-remaster path while the GRD recovery still holds the CF shard
 *	FROZEN (CF(S) itself unavailable), breaking the
 *	remaster -> unfreeze -> CF-shard-NORMAL deadlock.
 */
ClusterControlRootResult
cluster_control_root_read_canonical_dead_origin(uint16 origin_thread_id,
												ClusterControlRootSnapshot *out_snapshot)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootResult result;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	/* A failed PRE2 writer does not freeze the shared root: the recovery
	 * owner and other threads may still publish it. No lock-free fallback. */
	if (cluster_shared_config || origin_thread_id == 0
		|| origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = storage_contract_check(NULL, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;

	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& !primary->present[origin_thread_id - 1])
		result = CLUSTER_CONTROL_ROOT_ABSENT;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (out_snapshot != NULL)
			*out_snapshot = primary->records[origin_thread_id - 1];
	}
	pfree(bak);
	pfree(primary);
	return result;
}

ClusterControlRootResult
cluster_control_root_lookup_owner_by_node_runtime(int32 old_node_id,
												  ClusterControlRootIdentity *out_identity,
												  ClusterControlRootSnapshot *out_snapshot,
												  ClusterControlRootReadToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult result;
	uint16 thread_id;

	if (out_identity != NULL)
		memset(out_identity, 0, sizeof(*out_identity));
	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (old_node_id < 0 || old_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	thread_id = (uint16)(old_node_id + 1);
	if (cluster_shared_config) {
		result = read_canonical_v2(thread_id, NULL, &snapshot, &token);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
			if (snapshot.identity.origin_node_id != old_node_id)
				return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			if (out_identity != NULL)
				*out_identity = snapshot.identity;
			if (out_snapshot != NULL)
				*out_snapshot = snapshot;
			if (out_token != NULL)
				*out_token = token;
		}
		return result;
	}
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& (!primary->present[thread_id - 1]
			|| primary->records[thread_id - 1].identity.origin_node_id != old_node_id))
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		identity = primary->records[thread_id - 1].identity;
		snapshot = primary->records[thread_id - 1];
		make_read_token(primary, thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &token);
	}
	pfree(bak);
	pfree(primary);
	result = release_cf(ShareLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (out_identity != NULL)
			*out_identity = identity;
		if (out_snapshot != NULL)
			*out_snapshot = snapshot;
		if (out_token != NULL)
			*out_token = token;
	}
	return result;
}

static bool
encode_round(const ClusterControlRootMigrationRoundV1 *round, uint8 bytes[80])
{
	if (round == NULL || memcmp(round->magic, "PCRM", 4) != 0 || round->version != 1
		|| round->bytes != sizeof(*round) || round->prepare_generation == 0
		|| round->coordinator_incarnation == 0
		|| round->coordinator_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT || round->reserved76 != 0
		|| !cluster_control_root_feature_bitmap_is_known(round->source_feature_bitmap)
		|| !cluster_control_root_feature_bitmap_is_known(round->target_feature_bitmap)
		/* STOP04 §11.7: this package has no selected/certified production
		 * provider, so a migration image must not activate bit24. */
		|| (round->target_feature_bitmap & PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1) != 0
		|| (round->target_feature_bitmap & PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1)
			   == 0)
		return false;
	memset(bytes, 0, 80);
	memcpy(bytes, "PCRM", 4);
	write_u16_le(bytes + 4, 1);
	write_u16_le(bytes + 6, 80);
	write_u64_le(bytes + 8, round->prepare_generation);
	write_u64_le(bytes + 16, round->transition_epoch);
	write_u64_le(bytes + 24, round->source_feature_bitmap);
	write_u64_le(bytes + 32, round->target_feature_bitmap);
	write_u64_le(bytes + 40, round->admitted_bitmap_low);
	write_u64_le(bytes + 48, round->admitted_bitmap_high);
	write_u64_le(bytes + 56, round->capability_sample_digest);
	write_u64_le(bytes + 64, round->coordinator_incarnation);
	write_u32_le(bytes + 72, round->coordinator_node_id);
	return true;
}

static bool
migration_image_validate(const ClusterControlRootMigrationImage *image,
						 const ClusterControlRootMigrationRoundV1 *round)
{
	uint32 assigned = 0;
	uint16 i;

	if (image == NULL || round == NULL || image->system_identifier == 0
		|| image->system_identifier != GetSystemIdentifier() || image->reserved52 != 0
		|| image->assigned_record_count > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| !uuid_v4_valid(image->authority_uuid))
		return false;
	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
		const ClusterControlRootSnapshot *snapshot = &image->records[i];

		if (snapshot->identity.system_identifier == 0) {
			if (!bytes_are_zero(snapshot, sizeof(*snapshot)))
				return false;
			continue;
		}
		if (snapshot_validate(snapshot, (uint16)(i + 1), image->system_identifier,
							  image->storage_uuid, image->authority_uuid, false)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return false;
		if (snapshot->identity.root_lineage_seq != 1)
			return false;
		if (snapshot->lifecycle_reason != CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT)
			return false;
		assigned++;
	}
	return assigned == image->assigned_record_count;
}

static bool
read_thread_claim_exact(uint16 thread_id, int32 node_id, const ClusterControlRootIdentity *expected)
{
	ClusterWalThreadClaim claim;
	char dirname[MAXPGPATH];
	char dirpath[MAXPGPATH];
	char path[MAXPGPATH];
	struct stat st;
	size_t done = 0;
	int fd;
	int written;

	cluster_wal_thread_dir_name(thread_id, dirname, sizeof(dirname));
	written = snprintf(dirpath, sizeof(dirpath), "%s/%s", cluster_wal_threads_dir, dirname);
	if (dirname[0] == '\0' || written <= 0 || (size_t)written >= sizeof(dirpath)
		|| lstat(dirpath, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;
	written = snprintf(path, sizeof(path), "%s/%s", dirpath, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	if (written <= 0 || (size_t)written >= sizeof(path)
		|| !regular_or_absent_nosymlink(path, false))
		return false;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != sizeof(claim)) {
		if (fd >= 0)
			CloseTransientFile(fd);
		return false;
	}
	while (done < sizeof(claim)) {
		ssize_t n = read(fd, (uint8 *)&claim + done, sizeof(claim) - done);

		if (n <= 0) {
			CloseTransientFile(fd);
			return false;
		}
		done += (size_t)n;
	}
	if (CloseTransientFile(fd) != 0
		|| !cluster_wal_thread_claim_validate(&claim, thread_id, node_id, NULL)
		|| !bytes_are_zero(claim._pad_12, sizeof(claim._pad_12))
		|| !bytes_are_zero(claim._reserved_24, sizeof(claim._reserved_24))
		|| !bytes_are_zero(claim._pad_36, sizeof(claim._pad_36)))
		return false;
	return claim.created_at == expected->thread_claim_created_at
		   && claim.crc == expected->thread_claim_crc32c;
}

static ClusterControlRootResult
read_source_wal_state(const ClusterControlRootSnapshot *expected_records,
					  const uint8 *expected_hash, uint8 hash_out[PG_SHA256_DIGEST_LENGTH],
					  const ClusterControlRootMigrationRoundV1 *round)
{
	uint8 *first;
	uint8 *second;
	char path[MAXPGPATH];
	struct stat st;
	uint16 bad_thread;
	const char *reason;
	size_t done;
	int fd;
	int pass;
	uint16 i;
	uint32 assigned = 0;
	uint32 expected_assigned = 0;

	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| lstat(cluster_wal_threads_dir, &st) != 0 || !S_ISDIR(st.st_mode)
		|| snprintf(path, sizeof(path), "%s/%s", cluster_wal_threads_dir,
					CLUSTER_WAL_STATE_FILENAME)
			   <= 0
		|| !regular_or_absent_nosymlink(path, false))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	first = palloc(CLUSTER_WAL_STATE_FILE_SIZE);
	second = palloc(CLUSTER_WAL_STATE_FILE_SIZE);
	for (pass = 0; pass < 2; pass++) {
		uint8 *dst = pass == 0 ? first : second;

		fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
		if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != CLUSTER_WAL_STATE_FILE_SIZE) {
			if (fd >= 0)
				CloseTransientFile(fd);
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_BAD_SIZE;
		}
		done = 0;
		while (done < CLUSTER_WAL_STATE_FILE_SIZE) {
			ssize_t n = read(fd, dst + done, CLUSTER_WAL_STATE_FILE_SIZE - done);

			if (n <= 0) {
				CloseTransientFile(fd);
				pfree(second);
				pfree(first);
				return CLUSTER_CONTROL_ROOT_IO_ERROR;
			}
			done += (size_t)n;
		}
		if (CloseTransientFile(fd) != 0
			|| !cluster_wal_state_image_validate(dst, CLUSTER_WAL_STATE_FILE_SIZE, &bad_thread,
												 &reason)) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		}
	}
	if (memcmp(first, second, CLUSTER_WAL_STATE_FILE_SIZE) != 0) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	}
	for (i = 0; i < CLUSTER_WAL_STATE_SLOT_COUNT; i++) {
		ClusterWalStateSlot slot;
		ClusterWalSlotVerdict verdict;
		const ClusterControlRootSnapshot *expected = &expected_records[i];

		if (expected->identity.system_identifier != 0)
			expected_assigned++;
		memcpy(&slot, first + CLUSTER_WAL_STATE_SLOT_OFFSET(i + 1), sizeof(slot));
		verdict = cluster_wal_state_slot_classify(&slot, (uint16)(i + 1), -1, NULL);
		if (verdict == CLUSTER_WAL_SLOT_EMPTY) {
			if (expected->identity.system_identifier != 0) {
				pfree(second);
				pfree(first);
				return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
			}
			continue;
		}
		if (verdict != CLUSTER_WAL_SLOT_OK
			|| (slot.state != CLUSTER_WAL_SLOT_STATE_STOPPED
				&& !(slot.state == CLUSTER_WAL_SLOT_STATE_ACTIVE && round != NULL
					 && cluster_r4_bit22_source_close_current(round->transition_epoch,
															  round->prepare_generation)))
			|| slot.checkpoint_redo_lsn == 0 || slot.merge_recovered_lsn != 0
			|| expected->identity.system_identifier == 0
			|| expected->identity.origin_node_id != slot.node_id
			|| !read_thread_claim_exact((uint16)(i + 1), slot.node_id, &expected->identity)) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		}
		assigned++;
	}
	if (assigned != expected_assigned) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	}
	if (!control_root_sha256(first, CLUSTER_WAL_STATE_FILE_SIZE, hash_out)) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	pfree(second);
	pfree(first);
	if (expected_hash != NULL && memcmp(expected_hash, hash_out, PG_SHA256_DIGEST_LENGTH) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
fsync_parent_global(void)
{
	char path[MAXPGPATH];
	int fd;
	bool fsync_ok;
	bool close_ok;

	if (snprintf(path, sizeof(path), "%s/global", cluster_shared_data_dir) <= 0)
		return false;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0)
		return false;
	fsync_ok = pg_fsync(fd) == 0;
	close_ok = CloseTransientFile(fd) == 0;
	return fsync_ok && close_ok;
}

static bool
write_durable_image(const char *final_path, const uint8 *bytes)
{
	uint8 random_bytes[8];
	char temp_path[MAXPGPATH];
	char suffix[17];
	const char *base;
	size_t done = 0;
	int fd = -1;
	int i;
	bool created = false;
	bool renamed = false;
	bool ok = false;

	if (!regular_or_absent_nosymlink(final_path, true) || !pg_strong_random(random_bytes, 8))
		return false;
	for (i = 0; i < 8; i++)
		snprintf(suffix + i * 2, 3, "%02x", random_bytes[i]);
	suffix[16] = '\0';
	base = strrchr(final_path, '/');
	if (base == NULL)
		return false;
	if (snprintf(temp_path, sizeof(temp_path), "%.*s/%s.tmp.%d.%ld.%s", (int)(base - final_path),
				 final_path, base + 1, cluster_node_id, (long)getpid(), suffix)
		<= 0)
		return false;
	fd = BasicOpenFilePerm(temp_path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, 0600);
	if (fd < 0)
		return false;
	created = true;
	while (done < CLUSTER_CONTROL_ROOT_FILE_BYTES) {
		ssize_t n = write(fd, bytes + done, CLUSTER_CONTROL_ROOT_FILE_BYTES - done);

		if (n <= 0)
			goto cleanup;
		done += (size_t)n;
	}
	if (pg_fsync(fd) != 0) {
		(void)close(fd);
		fd = -1;
		goto cleanup_closed;
	}
	if (close(fd) != 0) {
		fd = -1;
		goto cleanup_closed;
	}
	fd = -1;
	if (durable_rename(temp_path, final_path, LOG) != 0)
		goto cleanup_closed;
	renamed = true;
	if (!fsync_parent_global())
		goto cleanup_closed;
	{
		uint8 *readback = palloc(CLUSTER_CONTROL_ROOT_FILE_BYTES);
		ClusterControlRootResult result = read_exact_file(final_path, readback);

		ok = result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			 && memcmp(readback, bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) == 0;
		pfree(readback);
	}
	return ok;

cleanup:
	(void)close(fd);
	fd = -1;
cleanup_closed:
	if (created && !renamed)
		(void)unlink(temp_path);
	return false;
}

static bool
publish_initial_image(const ControlRootImage *image)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];
	struct stat st;

	if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return false;
	if (lstat(primary, &st) == 0 || errno != ENOENT)
		return false;
	if (lstat(bak, &st) == 0 || errno != ENOENT)
		return false;
	return write_durable_image(bak, image->bytes) && write_durable_image(primary, image->bytes);
}

static bool
publish_updated_image(const ControlRootImage *old_image, const ControlRootImage *new_image)
{
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
		|| !build_control_path(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH))
		return false;
	return write_durable_image(bak, old_image->bytes)
		   && write_durable_image(primary, new_image->bytes);
}

static void
make_file_token(const ControlRootImage *image, ClusterControlRootFileToken *token)
{
	memset(token, 0, sizeof(*token));
	memcpy(token->authority_uuid, image->header.authority_uuid, 16);
	token->file_txn_seq = image->header.file_txn_seq;
	token->body_crc32c = image->header.body_crc32c;
	token->header_crc32c = image->header.header_crc32c;
	token->activation_state = image->header.activation_state;
	token->format_version = image->header.format_version;
	token->record_count = CLUSTER_CONTROL_ROOT_RECORD_COUNT;
	token->system_identifier = image->header.system_identifier;
	if (!control_root_sha256(image->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES, token->image_sha256))
		memset(token, 0, sizeof(*token));
}

static bool
file_token_equal(const ClusterControlRootFileToken *left, const ClusterControlRootFileToken *right)
{
	return left != NULL && right != NULL && memcmp(left, right, sizeof(*left)) == 0;
}

/* PGRAC: a normal checkpoint is an owner operation, not a generic root patch.
 * Root freshness, runtime authority and local WAL durability are independent.
 * No caller-provided flag can stand in for any of them.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static bool
checkpoint_v2_owner_current(const ClusterControlRootIdentity *self, uint64 epoch, TimeLineID tli,
							XLogRecPtr checkpoint_end)
{
	TimeLineID flushed_tli = 0;
	XLogRecPtr flushed;

	if (!AmCheckpointerProcess() || !cluster_enabled || !cluster_controlfile_shared_authority
		|| self->origin_node_id != cluster_node_id || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES
		|| self->origin_thread_id != (uint16)(cluster_node_id + 1)
		|| self->origin_owner_incarnation == 0
		|| cluster_qvotec_get_self_incarnation() != self->origin_owner_incarnation
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id)
			   != self->origin_owner_incarnation
		|| !cluster_wal_thread_dir_validated() || cluster_wal_thread_id() != self->origin_thread_id
		|| cluster_epoch_get_current() != epoch || cluster_reconfig_has_pending_prebump_stage()
		|| !cluster_serving_ready_is_current() || !cluster_write_fence_allowed())
		return false;
	flushed = GetFlushRecPtr(&flushed_tli);
	return flushed_tli == tli && flushed >= checkpoint_end && cluster_epoch_get_current() == epoch;
}

typedef enum CheckpointV2Purpose {
	CHECKPOINT_V2_ONLINE,
	CHECKPOINT_V2_SHUTDOWN_EVIDENCE
} CheckpointV2Purpose;

typedef struct CheckpointV2Work {
	CheckpointV2Purpose purpose;
	ControlRootImage base;
	ControlRootImage next;
	ControlFileData old_view;
	ControlFileData new_view;
	ClusterControlRootReadToken thread_token;
	ClusterControlRootFileToken before, after;
	ClusterRecoveryAnchorStageV2 stage;
	ClusterWalRootPublishGuard *walr;
	LOCKMODE cf_mode;
	/* PGRAC: exact generation WAL readback, released on all return/ERROR paths. */
	int wal_dirs[3];
	/* A native checkpoint fits within two pages, hence at most two segments. */
	int wal_segments[2];
	XLogSegNo wal_segment_numbers[2];
	XLogReaderState *wal_reader;
	uint16 wal_thread;
	TimeLineID wal_tli;
	ClusterControlRootResult wal_read_result;
	ClusterWalDurablePrefixRef prefix_ref;
	ClusterWalDurablePrefix prefix;
	uint32 checkpoint_crc;
} CheckpointV2Work;

/* A readable record plus an in-memory flush LSN is not the durable promise.
 * Consume the exact selected writer's current promise as a separate gate.
 * Later group flushes may advance it while CF is held: do not mistake those
 * for a root race, but never accept a regression or same-sequence divergence.
 * Root still advertises only the checkpoint record verified below, not the
 * unconsumed WAL between that checkpoint and a newer prefix end. */
static ClusterControlRootResult
checkpoint_v2_prefix_observe(CheckpointV2Work *work, const ControlFileData *control, XLogRecPtr end,
							 uint32 crc)
{
	ClusterWalDurablePrefix fresh;
	const ClusterWalDurablePrefix *previous = &work->prefix;
	ClusterControlRootResult result
		= cluster_wal_durable_prefix_read(cluster_wal_threads_dir, &work->prefix_ref, &fresh);

	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (fresh.exclusive_end < end
		|| (work->purpose == CHECKPOINT_V2_SHUTDOWN_EVIDENCE && fresh.exclusive_end != end))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (fresh.exclusive_end == end
		&& (fresh.record_start != control->checkPoint || fresh.record_crc != crc))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	/* Even the first observation must not claim that a later final record
	 * overlaps the checkpoint whose exact end was independently decoded. */
	if (fresh.exclusive_end > end && fresh.record_start < end)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (previous->sequence != 0) {
		if (fresh.sequence == previous->sequence) {
			if (fresh.exclusive_end != previous->exclusive_end
				|| fresh.record_start != previous->record_start
				|| fresh.record_crc != previous->record_crc)
				return CLUSTER_CONTROL_ROOT_COPY_DIVERGENT;
		} else if (fresh.sequence < previous->sequence
				   || fresh.exclusive_end <= previous->exclusive_end
				   || fresh.record_start < previous->exclusive_end)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	work->prefix = fresh;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static bool
checkpoint_wal_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode) && st->st_nlink == 1)
		   && st->st_uid == geteuid() && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

/* Keep each segment pinned across all decoder callbacks and publication. In
 * particular, a verified long header and its short pages must not come from
 * different inodes after a pathname replacement. Work cleanup owns every fd,
 * including on ERROR. Missing segments are a refusal, never natural EOF. */
static int
checkpoint_v2_wal_page(XLogReaderState *reader, XLogRecPtr pageptr, int required,
					   XLogRecPtr recordptr pg_attribute_unused(), char *page)
{
	CheckpointV2Work *work = reader->private_data;
	XLogSegNo segno;
	char filename[MAXFNAMELEN];
	struct stat st;
	int fd = -1;
	size_t used = 0;
	XLogPageHeader header;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;

	if (required < 0 || required > XLOG_BLCKSZ || pageptr % XLOG_BLCKSZ != 0)
		return XLREAD_FAIL;
	XLByteToSeg(pageptr, segno, reader->segcxt.ws_segsize);
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i) {
		if (work->wal_segments[i] >= 0) {
			if (work->wal_segment_numbers[i] == segno) {
				fd = work->wal_segments[i];
				break;
			}
			continue;
		}
		XLogFileName(filename, work->wal_tli, segno, reader->segcxt.ws_segsize);
		fd = openat(work->wal_dirs[2], filename,
					O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
		if (fd < 0) {
			work->wal_read_result = errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : result;
			return XLREAD_FAIL;
		}
		work->wal_segments[i] = fd;
		work->wal_segment_numbers[i] = segno;
		break;
	}
	if (fd < 0) {
		work->wal_read_result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		return XLREAD_FAIL;
	}
	if (fstat(fd, &st) != 0 || !checkpoint_wal_owned(&st, false))
		goto done;
	if (st.st_size != reader->segcxt.ws_segsize) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used < XLOG_BLCKSZ) {
		ssize_t count = pread(fd, page + used, XLOG_BLCKSZ - used,
							  XLogSegmentOffset(pageptr, reader->segcxt.ws_segsize) + used);
		if (count < 0 && errno == EINTR)
			continue;
		if (count <= 0)
			goto done;
		used += count;
	}
	header = (XLogPageHeader)page;
	if (header->xlp_thread_id != work->wal_thread || header->xlp_tli != work->wal_tli) {
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		goto done;
	}
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	work->wal_read_result = result;
	return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? XLOG_BLCKSZ : XLREAD_FAIL;
}

static bool
checkpoint_wal_matches(const CheckPoint *actual, const CheckPoint *expected)
{
#define CHECK_FIELD(field)                                                                         \
	if (actual->field != expected->field)                                                          \
	return false
	CHECK_FIELD(redo);
	CHECK_FIELD(ThisTimeLineID);
	CHECK_FIELD(PrevTimeLineID);
	CHECK_FIELD(fullPageWrites);
	if (!FullTransactionIdEquals(actual->nextXid, expected->nextXid))
		return false;
	CHECK_FIELD(nextOid);
	CHECK_FIELD(nextMulti);
	CHECK_FIELD(nextMultiOffset);
	CHECK_FIELD(oldestXid);
	CHECK_FIELD(oldestXidDB);
	CHECK_FIELD(oldestMulti);
	CHECK_FIELD(oldestMultiDB);
	CHECK_FIELD(time);
	CHECK_FIELD(oldestCommitTsXid);
	CHECK_FIELD(newestCommitTsXid);
	CHECK_FIELD(oldestActiveXid);
#undef CHECK_FIELD
	return true;
}

static bool
checkpoint_v2_wal_paths_current(const CheckpointV2Work *work,
								const ClusterControlRootIdentity *self)
{
	char thread[32], generation[48];
	const char *parts[] = { thread, generation };
	struct stat current, pinned;

	snprintf(thread, sizeof(thread), "thread_%u", self->origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 self->origin_owner_incarnation);
	for (size_t i = 0; i < lengthof(work->wal_dirs); ++i) {
		if (work->wal_dirs[i] < 0 || fstat(work->wal_dirs[i], &pinned) != 0
			|| (i == 0
					? lstat(cluster_wal_threads_dir, &current)
					: fstatat(work->wal_dirs[i - 1], parts[i - 1], &current, AT_SYMLINK_NOFOLLOW))
				   != 0
			|| !checkpoint_wal_owned(&current, true) || !checkpoint_wal_owned(&pinned, true)
			|| current.st_dev != pinned.st_dev || current.st_ino != pinned.st_ino)
			return false;
	}
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i) {
		char filename[MAXFNAMELEN];

		if (work->wal_segments[i] < 0)
			continue;
		XLogFileName(filename, work->wal_tli, work->wal_segment_numbers[i],
					 work->wal_reader->segcxt.ws_segsize);
		if (fstat(work->wal_segments[i], &pinned) != 0
			|| fstatat(work->wal_dirs[2], filename, &current, AT_SYMLINK_NOFOLLOW) != 0
			|| !checkpoint_wal_owned(&current, false) || !checkpoint_wal_owned(&pinned, false)
			|| current.st_dev != pinned.st_dev || current.st_ino != pinned.st_ino
			|| pinned.st_size != work->wal_reader->segcxt.ws_segsize
			|| current.st_size != pinned.st_size)
			return false;
	}
	return true;
}

static ClusterControlRootResult
checkpoint_v2_wal_verify(CheckpointV2Work *work, const ClusterControlRootIdentity *self,
						 const ControlFileData *control, XLogRecPtr end)
{
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	char thread[32], generation[48];
	const char *parts[] = { thread, generation };
	struct stat st;
	XLogRecord *record;
	CheckPoint checkpoint;
	char *error = NULL;

	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| !IsValidWalSegSize(wal_segment_size))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	snprintf(thread, sizeof(thread), "thread_%u", self->origin_thread_id);
	snprintf(generation, sizeof(generation), "generation_" UINT64_FORMAT,
			 self->origin_owner_incarnation);
	work->wal_dirs[0] = open(cluster_wal_threads_dir, flags);
	if (work->wal_dirs[0] < 0 || fstat(work->wal_dirs[0], &st) != 0
		|| !checkpoint_wal_owned(&st, true))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	for (size_t i = 0; i < lengthof(parts); ++i) {
		work->wal_dirs[i + 1] = openat(work->wal_dirs[i], parts[i], flags);
		if (work->wal_dirs[i + 1] < 0 || fstat(work->wal_dirs[i + 1], &st) != 0
			|| !checkpoint_wal_owned(&st, true))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	work->wal_thread = self->origin_thread_id;
	work->wal_tli = control->checkPointCopy.ThisTimeLineID;
	work->wal_read_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	work->wal_reader = XLogReaderAllocate(wal_segment_size, NULL,
										  XL_ROUTINE(.page_read = checkpoint_v2_wal_page), work);
	if (work->wal_reader == NULL)
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	work->wal_reader->system_identifier = self->system_identifier;
	work->wal_reader->cluster_expected_thread_id = self->origin_thread_id;
	work->wal_reader->seg.ws_tli = work->wal_tli;
	XLogBeginRead(work->wal_reader, control->checkPoint);
	record = XLogReadRecord(work->wal_reader, &error);
	if (work->wal_read_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return work->wal_read_result;
	if (record == NULL || work->wal_reader->ReadRecPtr != control->checkPoint
		|| work->wal_reader->EndRecPtr != end || record->xl_rmid != RM_XLOG_ID
		|| (record->xl_info & ~XLR_INFO_MASK)
			   != (work->purpose == CHECKPOINT_V2_ONLINE ? XLOG_CHECKPOINT_ONLINE
														 : XLOG_CHECKPOINT_SHUTDOWN)
		|| XLogRecGetDataLen(work->wal_reader) != sizeof(CheckPoint)
		|| XLogRecHasAnyBlockRefs(work->wal_reader))
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	memcpy(&checkpoint, XLogRecGetData(work->wal_reader), sizeof(checkpoint));
	work->checkpoint_crc = record->xl_crc;
	return checkpoint_wal_matches(&checkpoint, &control->checkPointCopy)
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}

static ClusterControlRootResult
checkpoint_v2_cleanup(CheckpointV2Work *work, ClusterControlRootResult result)
{
	ClusterControlRootResult discarded;

	if (work->wal_reader != NULL) {
		XLogReaderFree(work->wal_reader);
		work->wal_reader = NULL;
	}
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i) {
		int fd = work->wal_segments[i];
		work->wal_segments[i] = -1;
		if (fd >= 0 && close(fd) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	for (size_t i = 0; i < lengthof(work->wal_dirs); ++i) {
		int fd = work->wal_dirs[i];
		work->wal_dirs[i] = -1;
		if (fd >= 0 && close(fd) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	if (work->cf_mode != NoLock) {
		LOCKMODE mode = work->cf_mode;

		work->cf_mode = NoLock;
		result = release_cf(mode, result);
	}
	if (work->walr != NULL
		&& cluster_wal_retention_root_publish_end(&work->walr) != CLUSTER_WALR_RELEASE_CONFIRMED)
		result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	/* Discard only the operation's own temporary entry, never a formal object.
	 * This is also safe after root publication or ERROR during CF acquisition. */
	if (work->stage.owner_pid != 0) {
		discarded = cluster_recovery_anchor_v2_discard(&work->stage);
		if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| result == CLUSTER_CONTROL_ROOT_CAS_CONFLICT))
			result = discarded;
	}
	return result;
}

static ClusterControlRootResult
checkpoint_v2_publish_work(CheckpointV2Work *work, const ClusterControlRootIdentity *self,
						   const ControlFileData *cf, XLogRecPtr end, uint64 epoch)
{
	ClusterControlRootResult result;
	ClusterControlRootFileToken actual;
	ClusterControlRootSnapshot *record;
	ClusterRecoveryAnchorV2 anchor;
	ClusterWalPinResult walr_result;
	uint8 uuid[16];
	uint32 crc;
	int index = self->origin_thread_id - 1;

	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ShareLock;
	result = cluster_control_root_v2_read_thread_locked(self, &work->base, &work->old_view,
														&work->before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	record = &work->base.records[index];
	if (work->base.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| work->base.header.v2.database_state != 3
		|| (work->base.header.v2.serving[cluster_node_id / 64]
			& (UINT64_C(1) << (cluster_node_id % 64)))
			   == 0
		|| record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| work->old_view.state != DB_IN_PRODUCTION)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (work->base.header.file_txn_seq == UINT64_MAX || record->root_publish_seq == UINT64_MAX
		|| work->base.refs[index].anchor_generation == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	if (cf->checkPointCopy.ThisTimeLineID != record->checkpoint_tli
		|| cf->checkPointCopy.PrevTimeLineID != work->old_view.checkPointCopy.PrevTimeLineID
		|| cf->checkPointCopy.redo <= record->checkpoint_lower_lsn
		|| cf->checkPoint <= work->old_view.checkPoint || end < record->validated_tail_lsn_exclusive
		|| cf->minRecoveryPoint != work->old_view.minRecoveryPoint
		|| cf->minRecoveryPointTLI != work->old_view.minRecoveryPointTLI
		|| cf->unloggedLSN < work->old_view.unloggedLSN || cf->wal_level != work->old_view.wal_level
		|| cf->wal_log_hints != work->old_view.wal_log_hints
		|| cf->track_commit_timestamp != work->old_view.track_commit_timestamp)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	work->prefix_ref.claim.identity = record->identity;
	work->prefix_ref.claim.database_incarnation = work->base.header.v2.database_incarnation;
	work->prefix_ref.claim.max_config_generation = work->base.header.v2.config_generation;
	memcpy(work->prefix_ref.claim.claim_sha256, work->base.refs[index].claim_sha256, 32);
	work->prefix_ref.timeline = record->checkpoint_tli;
	make_read_token(&work->base, self->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->thread_token);
	work->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;

	memset(&anchor, 0, sizeof(anchor));
	anchor.identity = *self;
	anchor.database_incarnation = work->base.header.v2.database_incarnation;
	anchor.config_generation = work->base.header.v2.config_generation;
	anchor.anchor_generation = work->base.refs[index].anchor_generation + 1;
	memcpy(anchor.claim_sha256, work->base.refs[index].claim_sha256, 32);
	anchor.state = cf->state;
	anchor.write_time = cf->time;
	anchor.checkpoint = cf->checkPoint;
	anchor.checkpoint_copy = cf->checkPointCopy;
	anchor.unlogged_lsn = cf->unloggedLSN;
	anchor.min_recovery_point = cf->minRecoveryPoint;
	anchor.min_recovery_tli = cf->minRecoveryPointTLI;
	/* A normal checkpoint alone cannot prove every old parameter requirement
	 * retired. Keep the prior thread obligation; the explicit WAL/recovery
	 * coverage producer owns any later retirement, not current shared GUCs.
	 */
	/* These three are modes, not capacity bounds; a change needs the separate
	 * PARAMETER_CHANGE producer and was refused above. Never OR in capability.
	 */
	anchor.wal_log_hints = cf->wal_log_hints;
	anchor.track_commit_timestamp = cf->track_commit_timestamp;
	anchor.wal_level = cf->wal_level;
	anchor.max_connections = Max(cf->MaxConnections, work->old_view.MaxConnections);
	anchor.max_worker_processes
		= Max(cf->max_worker_processes, work->old_view.max_worker_processes);
	anchor.max_wal_senders = Max(cf->max_wal_senders, work->old_view.max_wal_senders);
	anchor.max_prepared_xacts = Max(cf->max_prepared_xacts, work->old_view.max_prepared_xacts);
	anchor.max_locks_per_xact = Max(cf->max_locks_per_xact, work->old_view.max_locks_per_xact);
	if (!pg_strong_random(uuid, sizeof(uuid)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	uuid[6] = (uuid[6] & 0x0f) | 0x40;
	uuid[8] = (uuid[8] & 0x3f) | 0x80;
	result = cluster_recovery_anchor_v2_prepare(&anchor, uuid, &work->stage);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	walr_result
		= cluster_wal_retention_root_publish_begin_exact(&work->thread_token, false, &work->walr);
	if (walr_result != CLUSTER_WAL_PIN_OK)
		return walr_result == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
													: CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = checkpoint_v2_wal_verify(work, self, cf, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	crc = work->checkpoint_crc;
	result = checkpoint_v2_prefix_observe(work, cf, end, crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ExclusiveLock;
	result
		= cluster_control_root_v2_read_thread_locked(self, &work->next, &work->new_view, &actual);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !checkpoint_v2_owner_current(self, epoch, cf->checkPointCopy.ThisTimeLineID, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!file_token_equal(&work->before, &actual))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = checkpoint_v2_prefix_observe(work, cf, end, crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_recovery_anchor_v2_install(&work->stage);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_recovery_anchor_v2_read_locked(&work->stage.ref, &work->old_view,
													&work->new_view);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	record = &work->next.records[index];
	record->checkpoint_tli = cf->checkPointCopy.ThisTimeLineID;
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	record->checkpoint_lower_lsn = cf->checkPointCopy.redo;
	record->checkpoint_record_crc32c = crc;
	record->root_flags
		|= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	record->tail_tli = record->checkpoint_tli;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->validated_tail_lsn_exclusive = end;
	record->tail_last_record_lsn = cf->checkPoint;
	record->tail_last_record_crc32c = crc;
	record->root_publish_seq++;
	record->published_at_usec = GetCurrentTimestamp();
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE;
	work->next.publisher_incarnation[index] = self->origin_owner_incarnation;
	work->next.publisher_node[index] = self->origin_node_id;
	work->next.refs[index].anchor_generation = work->stage.ref.anchor_generation;
	memcpy(work->next.refs[index].anchor_sha256, work->stage.ref.anchor_sha256, 32);
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = record->published_at_usec;
	result = cluster_control_root_v2_encode(&work->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !checkpoint_v2_owner_current(self, epoch, cf->checkPointCopy.ThisTimeLineID, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = checkpoint_v2_prefix_observe(work, cf, end, crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_control_root_v2_read_thread_locked(self, &work->base, &work->new_view,
														&work->after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (memcmp(work->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !checkpoint_v2_owner_current(self, epoch, cf->checkPointCopy.ThisTimeLineID, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = checkpoint_v2_prefix_observe(work, cf, end, crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* PGRAC: root is already durable and exactly reread. This compatibility
	 * output cannot roll it back, nor does it authorize native startup. Keep
	 * CF-X until the selected view and its directory entry are durable. */
	return cluster_cf_control_projection_write_locked(&work->new_view);
}

static ClusterControlRootResult
checkpoint_v2_publish(CheckpointV2Purpose purpose, const ClusterControlRootIdentity *self,
					  const ControlFileData *thread_control, XLogRecPtr checkpoint_end,
					  ClusterControlRootSnapshot *out, ClusterControlRootFileToken *out_token,
					  ControlFileData *out_control)
{
	CheckpointV2Work *work;
	ClusterControlRootResult result;
	uint64 epoch;

	if (out_control != NULL && out_control == thread_control)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out_control != NULL)
		memset(out_control, 0, sizeof(*out_control));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (self == NULL || thread_control == NULL || out == NULL || out_token == NULL
		|| out_control == NULL || !cluster_shared_config || !AmCheckpointerProcess() || !enableFsync
		|| self->origin_thread_id == 0 || self->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| thread_control->state
			   != (purpose == CHECKPOINT_V2_ONLINE ? DB_IN_PRODUCTION : DB_SHUTDOWNED)
		|| (purpose == CHECKPOINT_V2_SHUTDOWN_EVIDENCE
			&& thread_control->checkPointCopy.redo != thread_control->checkPoint)
		|| thread_control->backupStartPoint != 0 || thread_control->backupEndPoint != 0
		|| thread_control->backupEndRequired || thread_control->checkPoint == 0
		|| thread_control->wal_level < WAL_LEVEL_MINIMAL
		|| thread_control->wal_level > WAL_LEVEL_LOGICAL || thread_control->MaxConnections <= 0
		|| thread_control->max_worker_processes < 0 || thread_control->max_wal_senders < 0
		|| thread_control->max_prepared_xacts < 0 || thread_control->max_locks_per_xact <= 0
		|| thread_control->checkPointCopy.redo == 0
		|| thread_control->checkPointCopy.redo > thread_control->checkPoint
		|| checkpoint_end <= thread_control->checkPoint
		|| self->system_identifier != GetSystemIdentifier()
		|| thread_control->system_identifier != self->system_identifier
		|| cluster_cf_classify_buffer((const char *)thread_control, sizeof(*thread_control),
									  self->system_identifier)
			   != CLUSTER_CF_VALID)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* Never invert an outer CF hold into WALR acquisition. */
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	epoch = cluster_epoch_get_current();
	if (!checkpoint_v2_owner_current(self, epoch, thread_control->checkPointCopy.ThisTimeLineID,
									 checkpoint_end))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->purpose = purpose;
	for (size_t i = 0; i < lengthof(work->wal_dirs); ++i)
		work->wal_dirs[i] = -1;
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i)
		work->wal_segments[i] = -1;
	PG_TRY();
	{
		result = checkpoint_v2_publish_work(work, self, thread_control, checkpoint_end, epoch);
	}
	PG_CATCH();
	{
		(void)checkpoint_v2_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = checkpoint_v2_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out = work->base.records[self->origin_thread_id - 1];
		*out_token = work->after;
		*out_control = work->new_view;
	}
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_control_root_v2_checkpoint_publish(const ClusterControlRootIdentity *self,
										   const ControlFileData *thread_control,
										   XLogRecPtr checkpoint_end,
										   ClusterControlRootSnapshot *out,
										   ClusterControlRootFileToken *out_token,
										   ControlFileData *out_control)
{
	return checkpoint_v2_publish(CHECKPOINT_V2_ONLINE, self, thread_control, checkpoint_end, out,
								 out_token, out_control);
}

/* PGRAC: shutdown WAL and anchor evidence precede, but never substitute for,
 * the exact protocol/member clean-close publication. Root lifecycle and all
 * generic native projections stay OPEN until that separate owner completes.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_control_root_v2_shutdown_checkpoint_publish(const ClusterControlRootIdentity *self,
													const ControlFileData *thread_control,
													XLogRecPtr checkpoint_end,
													ClusterControlRootSnapshot *out,
													ClusterControlRootFileToken *out_token,
													ControlFileData *out_control)
{
	return checkpoint_v2_publish(CHECKPOINT_V2_SHUTDOWN_EVIDENCE, self, thread_control,
								 checkpoint_end, out, out_token, out_control);
}

/* PGRAC: failure input is a purpose-bound root transition, not a v1 patch
 * or permission inferred from a dead bitmap. All borrowed evidence remains
 * owned by the caller until our confirmed lock cleanup.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct FailureInputV2Work {
	ControlRootImage base;
	ControlRootImage next;
	ControlFileData control;
	ClusterControlRootReadToken token;
	ClusterWalRootPublishGuard *walr;
	ClusterRecoverySerialGuard serial;
	ClusterWalDurablePrefixRef prefix_ref;
	ClusterWalTailObservation tail;
	LOCKMODE cf_mode;
	int32 publisher_node;
	uint64 publisher_incarnation;
} FailureInputV2Work;

static bool
failure_v2_authorized(const ClusterRecoverySerialRequest *request, const FailureInputV2Work *work)
{
	ClusterRecoveryDutyDigest digest;
	PgracExternalFenceDenyReason reason;
	uint32 count;
	bool original_owner = false;

	if (cluster_node_id != work->publisher_node
		|| cluster_qvotec_get_self_incarnation() != work->publisher_incarnation
		|| work->publisher_incarnation == 0
		|| (work->publisher_node == request->duty.origin_node_id
			&& work->publisher_incarnation == request->duty.origin_owner_incarnation)
		|| !cluster_recovery_duty_digest_for_claim(&request->duty, true, &digest)
		|| cluster_formation_witness_revalidate_nowait(request->formation)
			   != CLUSTER_FORMATION_WITNESS_READY
		|| !cluster_external_fence_need_set_revalidate_nowait(request->fence_need_set,
															  request->formation, &reason)
		|| !cluster_external_fence_revalidate_set_nowait(
			request->fence_admission_set, request->fence_need_set, request->formation, &reason))
		return false;
	count = cluster_external_fence_need_set_count(request->fence_need_set);
	if (count == 0 || count > CLUSTER_MAX_NODES)
		return false;
	for (uint32 i = 0; i < count; i++) {
		const PgracExternalFenceNeedV1 *need
			= cluster_external_fence_need_set_at(request->fence_need_set, i);

		if (need == NULL || need->system_identifier != request->duty.system_identifier
			|| memcmp(&need->canonical_duty_digest, &digest, sizeof(digest)) != 0)
			return false;
		if (need->victim_node_id == request->duty.origin_node_id
			&& need->victim_incarnation == request->duty.origin_owner_incarnation)
			original_owner = true;
	}
	return original_owner;
}

static ClusterControlRootResult
failure_v2_cleanup(FailureInputV2Work *work, ClusterControlRootResult result)
{
	if (work->cf_mode != NoLock) {
		LOCKMODE mode = work->cf_mode;

		work->cf_mode = NoLock;
		result = release_cf(mode, result);
	}
	if ((work->serial.held || work->serial.release_uncertain)
		&& cluster_recovery_serial_release(&work->serial)
			   != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
		result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	if (work->walr != NULL
		&& cluster_wal_retention_root_publish_end(&work->walr) != CLUSTER_WALR_RELEASE_CONFIRMED)
		result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
		ereport(
			FATAL,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("failed-thread input publisher could not confirm lock release"),
			 errhint("Keep the failed thread closed until recovery ownership is re-established.")));
	return result;
}

static bool
failure_v2_current(const ClusterRecoverySerialRequest *request, FailureInputV2Work *work)
{
	return failure_v2_authorized(request, work)
		   && cluster_recovery_serial_input_revalidate(&work->serial)
				  == CLUSTER_RECOVERY_SERIAL_CURRENT;
}

/* PGRAC: observe under owned CF, but never keep CF over the WAL scan.
 * The full root token also detects changes to unrelated thread/common state.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult
failure_v2_tail_read(const ClusterRecoverySerialRequest *request, FailureInputV2Work *work,
					 LOCKMODE mode)
{
	ClusterControlRootResult result;
	ClusterControlRootFileToken file_token;
	const ClusterControlRootSnapshot *record;
	int index = request->duty.origin_thread_id - 1;

	if (!cluster_cf_lock(mode))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = mode;
	if (!cluster_cf_held_is_clusterwide(mode))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = cluster_control_root_v2_read_thread_locked(&request->duty, &work->base, &work->control,
														&file_token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	make_read_token(&work->base, request->duty.origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->token);
	if (!read_token_equal(&work->token, &request->expected_root_token))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	record = &work->base.records[index];
	if (work->base.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| work->base.header.v2.database_state < CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| work->base.header.v2.database_state > CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		|| record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (record->root_flags & ~CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF)
			   != (CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
				   | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID)
		|| (work->base.header.v2.serving[request->duty.origin_node_id / 64]
			& (UINT64_C(1) << (request->duty.origin_node_id % 64)))
			   != 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (work->base.header.file_txn_seq == UINT64_MAX || record->root_publish_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	return failure_v2_current(request, work) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
											 : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
failure_v2_tail_commit(const ClusterRecoverySerialRequest *request, FailureInputV2Work *work)
{
	ClusterControlRootResult result;
	ClusterControlRootFileToken file_token;
	ClusterWalDurablePrefix fresh;
	const ClusterWalDurablePrefix *observed = &work->tail.durable_prefix;
	ClusterControlRootSnapshot *record;
	int index = request->duty.origin_thread_id - 1;

	result = failure_v2_tail_read(request, work, ExclusiveLock);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_durable_prefix_read(cluster_wal_threads_dir, &work->prefix_ref, &fresh);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (fresh.sequence != observed->sequence || fresh.exclusive_end != observed->exclusive_end
		|| fresh.record_start != observed->record_start || fresh.record_crc != observed->record_crc)
		return CLUSTER_CONTROL_ROOT_COPY_DIVERGENT;
	work->next = work->base;
	record = &work->next.records[index];
	record->root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
						  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID
						  | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	record->tail_tli = record->checkpoint_tli;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->validated_tail_lsn_exclusive = work->tail.complete_end;
	record->tail_last_record_lsn = work->tail.last_record_start;
	record->tail_last_record_crc32c = work->tail.last_record_crc;
	record->recovered_tli = record->checkpoint_tli;
	record->recovered_through_lsn_exclusive = record->checkpoint_lower_lsn;
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED;
	record->root_publish_seq++;
	record->published_at_usec = GetCurrentTimestamp();
	work->next.publisher_node[index] = work->publisher_node;
	work->next.publisher_incarnation[index] = work->publisher_incarnation;
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = record->published_at_usec;
	result = cluster_control_root_v2_encode(&work->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!failure_v2_current(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_control_root_v2_read_thread_locked(&request->duty, &work->base, &work->control,
														&file_token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(work->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	if (!failure_v2_current(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	make_read_token(&work->base, request->duty.origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->token);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
failure_v2_tail_work(const ClusterRecoverySerialRequest *request, FailureInputV2Work *work)
{
	ClusterControlRootResult result;
	ClusterWalPinResult retained;
	ClusterRecoverySerialAcquireResult acquired;
	const ClusterControlRootSnapshot *record;
	int index = request->duty.origin_thread_id - 1;

	retained = cluster_wal_retention_root_publish_begin_exact(&request->expected_root_token, false,
															  &work->walr);
	if (retained != CLUSTER_WAL_PIN_OK)
		return retained == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
												 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	acquired = cluster_recovery_serial_acquire(request, &work->serial);
	if (acquired != CLUSTER_RECOVERY_SERIAL_GRANTED)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = failure_v2_tail_read(request, work, ShareLock);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	record = &work->base.records[index];
	if (work->control.checkPoint < record->checkpoint_lower_lsn
		|| work->control.checkPoint == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	work->prefix_ref.claim.identity = record->identity;
	work->prefix_ref.claim.database_incarnation = work->base.header.v2.database_incarnation;
	work->prefix_ref.claim.max_config_generation = work->base.header.v2.config_generation;
	memcpy(work->prefix_ref.claim.claim_sha256, work->base.refs[index].claim_sha256, 32);
	work->prefix_ref.timeline = record->checkpoint_tli;
	work->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_tail_observe_checkpoint(
		cluster_wal_threads_dir, &work->prefix_ref, wal_segment_size, record->checkpoint_lower_lsn,
		work->control.checkPoint + 1, work->control.checkPoint, record->checkpoint_record_crc32c,
		&work->tail);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!failure_v2_current(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return failure_v2_tail_commit(request, work);
}

static ClusterControlRootResult
failure_v2_open_work(const ClusterRecoverySerialRequest *request, FailureInputV2Work *work)
{
	ClusterControlRootResult result;
	ClusterWalPinResult retained;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot *record;
	int index = request->duty.origin_thread_id - 1;

	retained = cluster_wal_retention_root_publish_begin_exact(&request->expected_root_token, false,
															  &work->walr);
	if (retained != CLUSTER_WAL_PIN_OK)
		return retained == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
												 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!cluster_cf_lock(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ExclusiveLock;
	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = cluster_control_root_v2_read_thread_locked(&request->duty, &work->base, &work->control,
														&file_token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	make_read_token(&work->base, request->duty.origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->token);
	if (!read_token_equal(&work->token, &request->expected_root_token))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	record = &work->base.records[index];
	if (work->base.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| work->base.header.v2.database_state < CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| work->base.header.v2.database_state > CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		|| record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (work->base.header.file_txn_seq == UINT64_MAX || record->root_publish_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	if (!failure_v2_authorized(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	work->next = work->base;
	record = &work->next.records[index];
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	/* PGRAC: failure cannot erase the failed writer's unsafe-FPW history.
	 * Sealing input does not discharge this later recovery safety check. */
	record->root_flags = (record->root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF)
						 | CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						 | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	record->tail_tli = 0;
	record->tail_validation_kind = 0;
	record->validated_tail_lsn_exclusive = 0;
	record->tail_last_record_lsn = 0;
	record->tail_last_record_crc32c = 0;
	record->recovered_tli = 0;
	record->recovered_through_lsn_exclusive = 0;
	record->recovered_last_record_lsn = 0;
	record->recovered_last_record_crc32c = 0;
	record->conservative_bound_kind = CLUSTER_CONTROL_ROOT_BOUND_NONE;
	record->conservative_commit_scn = 0;
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_DUTY_OPEN;
	record->root_publish_seq++;
	record->published_at_usec = GetCurrentTimestamp();
	work->next.publisher_node[index] = work->publisher_node;
	work->next.publisher_incarnation[index] = work->publisher_incarnation;
	work->next.header.v2.serving[request->duty.origin_node_id / 64]
		&= ~(UINT64_C(1) << (request->duty.origin_node_id % 64));
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = record->published_at_usec;
	result = cluster_control_root_v2_encode(&work->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!failure_v2_authorized(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_control_root_v2_read_thread_locked(&request->duty, &work->base, &work->control,
														&file_token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(work->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	if (!failure_v2_authorized(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	make_read_token(&work->base, request->duty.origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->token);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
failure_v2_publish(const ClusterRecoverySerialRequest *request, ClusterControlRootSnapshot *out,
				   ClusterControlRootReadToken *out_token, bool seal_tail)
{
	FailureInputV2Work *work;
	ClusterControlRootResult result;
	ClusterRecoverySerialRequest saved_request;

	/* PGRAC: permit replacing the caller's expected token in place. */
	if (request != NULL) {
		saved_request = *request;
		request = &saved_request;
	}
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (request == NULL || out == NULL || out_token == NULL || !cluster_shared_config
		|| !cluster_enabled || !cluster_controlfile_shared_authority || !enableFsync
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES
		|| request->mode != CLUSTER_RECOVERY_SERIAL_INPUT_SEAL
		|| !cluster_recovery_duty_key_valid_for_claim(&request->duty, true)
		|| request->duty.system_identifier != GetSystemIdentifier() || request->formation == NULL
		|| request->fence_need_set == NULL || request->fence_admission_set == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work = palloc0(sizeof(*work));
	work->publisher_node = cluster_node_id;
	work->publisher_incarnation = cluster_qvotec_get_self_incarnation();
	PG_TRY();
	{
		if (!failure_v2_authorized(request, work))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else
			result = seal_tail ? failure_v2_tail_work(request, work)
							   : failure_v2_open_work(request, work);
	}
	PG_CATCH();
	{
		(void)failure_v2_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = failure_v2_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out = work->base.records[request->duty.origin_thread_id - 1];
		*out_token = work->token;
	}
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_control_root_v2_failure_open_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	return failure_v2_publish(request, out, out_token, false);
}

ClusterControlRootResult
cluster_control_root_v2_failure_tail_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	return failure_v2_publish(request, out, out_token, true);
}

/* PGRAC: the raw shutdown anchor and native insertion cut are independent
 * facts. Generic root views deliberately still report IN_PRODUCTION here.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
shutdown_v2_owner_current(const ClusterWalDurablePrefixRef *expected, uint64 epoch, XLogRecPtr end)
{
	return ShutdownRequestPending
		   && checkpoint_v2_owner_current(&expected->claim.identity, epoch, expected->timeline, end)
		   /* The next record START skips a page header at an exact boundary;
			* only the reserved END is comparable with this record's end. */
		   && GetXLogInsertEndRecPtr() == end;
}

static ClusterControlRootResult
shutdown_v2_observe_work(CheckpointV2Work *work, const ClusterWalDurablePrefixRef *expected,
						 uint64 epoch)
{
	const ClusterControlRootIdentity *self = &expected->claim.identity;
	ClusterRecoveryAnchorRefV2 anchor;
	ClusterControlRootSnapshot *record;
	ClusterControlRootResult result;
	ClusterWalPinResult walr_result;
	XLogRecPtr end;
	int index = self->origin_thread_id - 1;

	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ShareLock;
	result = cluster_control_root_v2_read_thread_locked(self, &work->base, &work->new_view,
														&work->before);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	record = &work->base.records[index];
	if (work->base.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| work->base.header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		|| record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| (work->base.header.v2.serving[cluster_node_id / 64]
			& (UINT64_C(1) << (cluster_node_id % 64)))
			   == 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (expected->claim.database_incarnation != work->base.header.v2.database_incarnation
		|| expected->claim.max_config_generation == 0
		|| expected->claim.max_config_generation > work->base.header.v2.config_generation
		|| expected->timeline != record->checkpoint_tli
		|| memcmp(expected->claim.claim_sha256, work->base.refs[index].claim_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	memset(&anchor, 0, sizeof(anchor));
	anchor.identity = *self;
	anchor.database_incarnation = work->base.header.v2.database_incarnation;
	anchor.max_config_generation = work->base.header.v2.config_generation;
	anchor.anchor_generation = work->base.refs[index].anchor_generation;
	memcpy(anchor.anchor_sha256, work->base.refs[index].anchor_sha256, 32);
	memcpy(anchor.claim_sha256, work->base.refs[index].claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_locked(&anchor, &work->new_view, &work->old_view);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	end = record->validated_tail_lsn_exclusive;
	if (work->old_view.state != DB_SHUTDOWNED
		|| work->old_view.checkPointCopy.redo != work->old_view.checkPoint
		|| record->checkpoint_lower_lsn != work->old_view.checkPoint
		|| (record->root_flags
			& (CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
			   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID))
			   != (CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
				   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID)
		|| record->tail_tli != record->checkpoint_tli
		|| record->tail_last_record_lsn != work->old_view.checkPoint
		|| record->tail_last_record_crc32c != record->checkpoint_record_crc32c
		|| end <= record->tail_last_record_lsn)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (!shutdown_v2_owner_current(expected, epoch, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	work->prefix_ref = *expected;
	make_read_token(&work->base, self->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->thread_token);
	work->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* WALR before CF: an exact current generation cannot be recycled while
	 * its native record and root-selected end are being consumed. */
	walr_result
		= cluster_wal_retention_root_publish_begin_exact(&work->thread_token, false, &work->walr);
	if (walr_result != CLUSTER_WAL_PIN_OK)
		return walr_result == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
													: CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = checkpoint_v2_wal_verify(work, self, &work->old_view, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->checkpoint_crc != record->checkpoint_record_crc32c)
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	result = checkpoint_v2_prefix_observe(work, &work->old_view, end, work->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ShareLock;
	result = cluster_control_root_v2_read_thread_locked(self, &work->next, &work->new_view,
														&work->after);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!file_token_equal(&work->before, &work->after))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = checkpoint_v2_prefix_observe(work, &work->old_view, end, work->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !shutdown_v2_owner_current(expected, epoch, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v2_shutdown_observe(const ClusterWalDurablePrefixRef *expected,
										 ClusterControlRootSnapshot *out,
										 ClusterControlRootFileToken *out_token)
{
	CheckpointV2Work *work;
	ClusterWalDurablePrefixRef ref;
	ClusterControlRootResult result;
	uint64 epoch;
	if (expected != NULL)
		ref = *expected;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (expected == NULL || out == NULL || out_token == NULL || !cluster_shared_config
		|| !AmCheckpointerProcess() || !ShutdownRequestPending || !enableFsync
		|| ref.claim.identity.origin_thread_id == 0
		|| ref.claim.identity.origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| ref.claim.identity.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	epoch = cluster_epoch_get_current();
	if (!checkpoint_v2_owner_current(&ref.claim.identity, epoch, ref.timeline, 0))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	work = palloc0(sizeof(*work));
	work->purpose = CHECKPOINT_V2_SHUTDOWN_EVIDENCE;
	for (size_t i = 0; i < lengthof(work->wal_dirs); ++i)
		work->wal_dirs[i] = -1;
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i)
		work->wal_segments[i] = -1;
	PG_TRY();
	{
		result = shutdown_v2_observe_work(work, &ref, epoch);
	}
	PG_CATCH();
	{
		(void)checkpoint_v2_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = checkpoint_v2_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out = work->base.records[ref.claim.identity.origin_thread_id - 1];
		*out_token = work->after;
	}
	pfree(work);
	return result;
}

/*
 * read_thread_claim_fields -- RF-ROOT P7 (contract, step ④d): read the
 * thread claim file (40-byte v1 layout) and extract the identity fields
 * (created_at / crc) for a migration-image record.  The claim is
 * write-once (spec-4.1), so this is the durable origin evidence.
 */
static bool
read_thread_claim_fields(uint16 thread_id, int32 node_id, ClusterControlRootIdentity *identity)
{
	ClusterWalThreadClaim claim;
	char dirname[MAXPGPATH];
	char dirpath[MAXPGPATH];
	char path[MAXPGPATH];
	struct stat st;
	size_t done = 0;
	int fd;

	if (identity == NULL)
		return false;
	cluster_wal_thread_dir_name(thread_id, dirname, sizeof(dirname));
	if (dirname[0] == '\0'
		|| snprintf(dirpath, sizeof(dirpath), "%s/%s", cluster_wal_threads_dir, dirname) <= 0
		|| (size_t)snprintf(dirpath, sizeof(dirpath), "%s/%s", cluster_wal_threads_dir, dirname)
			   >= sizeof(dirpath)
		|| lstat(dirpath, &st) != 0 || !S_ISDIR(st.st_mode))
		return false;
	if (snprintf(path, sizeof(path), "%s/%s", dirpath, CLUSTER_WAL_THREAD_CLAIM_FILENAME) <= 0
		|| (size_t)snprintf(path, sizeof(path), "%s/%s", dirpath, CLUSTER_WAL_THREAD_CLAIM_FILENAME)
			   >= sizeof(path))
		return false;
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size != sizeof(claim)) {
		if (fd >= 0)
			CloseTransientFile(fd);
		return false;
	}
	while (done < sizeof(claim)) {
		ssize_t n = read(fd, (uint8 *)&claim + done, sizeof(claim) - done);

		if (n <= 0) {
			CloseTransientFile(fd);
			return false;
		}
		done += (size_t)n;
	}
	if (CloseTransientFile(fd) != 0
		|| !cluster_wal_thread_claim_validate(&claim, thread_id, node_id, NULL))
		return false;
	identity->thread_claim_created_at = claim.created_at;
	identity->thread_claim_crc32c = claim.crc;
	return true;
}

/*
 * RF-ROOT P9 verification step-2 (contract): the thread-WAL-stream reader for
 * the migration image.  The canonical root record needs the checkpoint
 * record's CRC and the validated tail-last record; both live in the
 * per-thread WAL stream (cluster_wal_threads_dir/thread_N), read through
 * XLogReader with thread-local segment callbacks (the stream is NOT the
 * local pg_wal).
 */
typedef struct MigrationWalReaderPrivate {
	char thread_dir[MAXPGPATH];
	uint32 tli; /* stream TLI (from the wal-state slot) */
} MigrationWalReaderPrivate;

static void
migration_wal_segment_open(XLogReaderState *state, XLogSegNo nextSegNo, TimeLineID *tli_p)
{
	MigrationWalReaderPrivate *priv = (MigrationWalReaderPrivate *)state->private_data;
	char fname[MAXFNAMELEN];
	char path[MAXPGPATH];
	int written;

	if (priv == NULL) {
		state->seg.ws_file = -1;
		return;
	}
	XLogFileName(fname, *tli_p, nextSegNo, state->segcxt.ws_segsize);
	written = snprintf(path, sizeof(path), "%s/%s", priv->thread_dir, fname);
	if (written <= 0 || (size_t)written >= sizeof(path)) {
		state->seg.ws_file = -1;
		return;
	}
	state->seg.ws_file = BasicOpenFile(path, O_RDONLY | PG_BINARY);
}

static void
migration_wal_segment_close(XLogReaderState *state)
{
	/* RF-ROOT P9 verification / implementation review: the segment was opened
	 * with BasicOpenFile (a raw fd, NOT the OpenTransientFile virtual-fd
	 * table), so CloseTransientFile here is an API pairing violation
	 * ("fd passed to CloseTransientFile was not obtained from
	 * OpenTransientFile") — close() directly, matching PostgreSQL's own
	 * wal_segment_close(). */
	if (state->seg.ws_file >= 0)
		close(state->seg.ws_file);
	state->seg.ws_file = -1;
}

static int
migration_wal_read_page(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
						XLogRecPtr targetRecPtr, char *cur_page)
{
	MigrationWalReaderPrivate *priv = (MigrationWalReaderPrivate *)state->private_data;
	WALReadError errinfo;

	/* WALRead drives segment_open/close and pg_pread; a short read or a
	 * missing segment is a clean end of the stream (WOULDBLOCK), which the
	 * reader treats as EOF. */
	if (priv == NULL)
		return XLREAD_FAIL;
	if (!WALRead(state, cur_page, targetPagePtr, XLOG_BLCKSZ, priv->tli, &errinfo))
		return XLREAD_WOULDBLOCK;
	return XLOG_BLCKSZ;
}

/*
 * migration_wal_scan -- read the thread stream from the checkpoint redo
 * record up to the write position; extract the checkpoint record CRC and
 * the last complete record below the write position (tail-last).  A
 * missing checkpoint record or any read failure fails closed (the image
 * then cannot pass migration_image_validate — the round stays un-begun).
 */
static bool
migration_wal_scan(uint16 thread_id, uint32 tli, XLogRecPtr checkpoint_redo, XLogRecPtr write_pos,
				   uint32 *out_ckpt_crc, XLogRecPtr *out_tail_last_lsn, uint32 *out_tail_last_crc)
{
	MigrationWalReaderPrivate priv;
	XLogReaderState *reader;
	XLogRecPtr first_valid;
	const XLogRecord *record;
	char *errormsg;
	bool saw_checkpoint = false;

	if (out_ckpt_crc == NULL || out_tail_last_lsn == NULL || out_tail_last_crc == NULL || tli == 0
		|| XLogRecPtrIsInvalid(checkpoint_redo) || XLogRecPtrIsInvalid(write_pos)
		|| write_pos < checkpoint_redo)
		return false;
	*out_ckpt_crc = 0;
	*out_tail_last_lsn = 0;
	*out_tail_last_crc = 0;
	{
		char dirname[MAXPGPATH];

		memset(&priv, 0, sizeof(priv));
		priv.tli = tli;
		cluster_wal_thread_dir_name(thread_id, dirname, sizeof(dirname));
		if (dirname[0] == '\0'
			|| snprintf(priv.thread_dir, sizeof(priv.thread_dir), "%s/%s", cluster_wal_threads_dir,
						dirname)
				   <= 0
			|| (size_t)snprintf(priv.thread_dir, sizeof(priv.thread_dir), "%s/%s",
								cluster_wal_threads_dir, dirname)
				   >= sizeof(priv.thread_dir))
			return false;
	}
	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = &migration_wal_read_page,
										   .segment_open = &migration_wal_segment_open,
										   .segment_close = &migration_wal_segment_close),
								&priv);
	if (reader == NULL)
		return false;
	first_valid = XLogFindNextRecord(reader, checkpoint_redo);
	if (XLogRecPtrIsInvalid(first_valid)) {
		XLogReaderFree(reader);
		return false;
	}
	for (;;) {
		record = XLogReadRecord(reader, &errormsg);
		if (record == NULL)
			break; /* clean end of stream */
		if (!saw_checkpoint) {
			/* The first record at the redo pointer is the checkpoint record. */
			*out_ckpt_crc = record->xl_crc;
			saw_checkpoint = true;
		}
		if (reader->ReadRecPtr >= write_pos)
			break;
		if (reader->EndRecPtr <= write_pos) {
			*out_tail_last_lsn = reader->ReadRecPtr;
			*out_tail_last_crc = record->xl_crc;
		}
	}
	XLogReaderFree(reader);
	return saw_checkpoint && *out_ckpt_crc != 0;
}

/*
 * cluster_control_root_build_migration_image -- RF-ROOT P7 (contract, step
 * ④d) + P9 verification (verified implementation): construct the create_prepared
 * migration image from the live shared state: wal-state registry slots
 * (checkpoint/tail bounds), thread claim files (origin evidence),
 * membership incarnations (owner binding) and the local storage identity.
 * Fail-closed: every non-empty slot must be STOPPED with a valid
 * checkpoint and zero merge-recovered bytes — OR ACTIVE and frozen by the
 * same round's all-member source-close BARRIER
 * (cluster_r4_bit22_source_close_current(round)): the online first-open
 * round freezes every member's writers first, so an ACTIVE slot is
 * provably quiesced (no offline STOPPED requirement).  Anything else
 * refuses the round before any file is touched.
 */
ClusterControlRootResult
cluster_control_root_build_migration_image(const ClusterControlRootMigrationRoundV1 *round,
										   ClusterControlRootMigrationImage *out)
{
	ClusterControlRootMigrationImage image;
	uint8 current_uuid[16];
	uint8 *first;
	uint8 *second;
	char path[MAXPGPATH];
	struct stat st;
	uint32 assigned = 0;
	uint16 i;
	int fd;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	/* RF-ROOT P9 verification / implementation review: the LOCAL image is
	 * filled field-by-field; without zeroing it first the reserved fields
	 * carry stack garbage (observed live: reserved42=1 -> snapshot_
	 * validate BAD_RESERVED -> create_prepared INVALID_ARGUMENT on the
	 * very first bit22 round).  Zero it with out so every reserved byte is
	 * deterministic. */
	memset(&image, 0, sizeof(image));
	if (cluster_wal_threads_dir == NULL || cluster_wal_threads_dir[0] == '\0'
		|| lstat(cluster_wal_threads_dir, &st) != 0 || !S_ISDIR(st.st_mode)
		|| snprintf(path, sizeof(path), "%s/%s", cluster_wal_threads_dir,
					CLUSTER_WAL_STATE_FILENAME)
			   <= 0
		|| !regular_or_absent_nosymlink(path, false))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	first = palloc(CLUSTER_WAL_STATE_FILE_SIZE);
	second = palloc(CLUSTER_WAL_STATE_FILE_SIZE);
	/* Read the registry once and validate the image (its embedded checksum
	 * rejects a torn read — the same guarantee the double-read in
	 * read_source_wal_state provides, without re-reading per slot). */
	fd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != CLUSTER_WAL_STATE_FILE_SIZE) {
		if (fd >= 0)
			CloseTransientFile(fd);
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	}
	if (pg_pread(fd, first, CLUSTER_WAL_STATE_FILE_SIZE, 0) != CLUSTER_WAL_STATE_FILE_SIZE
		|| CloseTransientFile(fd) != 0
		|| !cluster_wal_state_image_validate(first, CLUSTER_WAL_STATE_FILE_SIZE, NULL, NULL)) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	}
	memcpy(second, first, CLUSTER_WAL_STATE_FILE_SIZE);
	image.system_identifier = GetSystemIdentifier();
	if (!current_storage_uuid(current_uuid)) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	}
	memcpy(image.storage_uuid, current_uuid, 16);
	if (!pg_strong_random(image.authority_uuid, 16)) {
		pfree(second);
		pfree(first);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	/* RFC-4122 version-4 stamp: migration_image_validate requires
	 * uuid_v4_valid (version 4 + RFC variant bits) on the image authority
	 * UUID; pg_strong_random alone yields arbitrary bytes (1/64 chance of
	 * accidentally satisfying the nibble checks), which failed the live
	 * first-open create_prepared (INVALID_ARGUMENT) at the very first
	 * bit22 round — the fixture cast path never hit this because it
	 * carries an externally-minted v4 UUID. */
	image.authority_uuid[6] = (uint8)((image.authority_uuid[6] & UINT8_C(0x0f)) | UINT8_C(0x40));
	image.authority_uuid[8] = (uint8)((image.authority_uuid[8] & UINT8_C(0x3f)) | UINT8_C(0x80));
	image.created_at_usec = GetCurrentTimestamp();
	for (i = 0; i < CLUSTER_WAL_STATE_SLOT_COUNT; i++) {
		ClusterWalStateSlot slot;
		ClusterWalSlotVerdict verdict;
		ClusterControlRootSnapshot *record;

		memcpy(&slot, first + CLUSTER_WAL_STATE_SLOT_OFFSET(i + 1), sizeof(slot));
		verdict = cluster_wal_state_slot_classify(&slot, (uint16)(i + 1), -1, NULL);
		if (verdict == CLUSTER_WAL_SLOT_EMPTY)
			continue;
		if (verdict != CLUSTER_WAL_SLOT_OK || slot.checkpoint_redo_lsn == 0
			|| slot.merge_recovered_lsn != 0
			|| (slot.state != CLUSTER_WAL_SLOT_STATE_STOPPED
				&& !(slot.state == CLUSTER_WAL_SLOT_STATE_ACTIVE && round != NULL
					 && cluster_r4_bit22_source_close_current(round->transition_epoch,
															  round->prepare_generation)))) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		}
		record = &image.records[i];
		record->identity.system_identifier = image.system_identifier;
		memcpy(record->identity.storage_uuid, image.storage_uuid, 16);
		memcpy(record->identity.authority_uuid, image.authority_uuid, 16);
		record->identity.origin_thread_id = (uint16)(i + 1);
		record->identity.origin_node_id = slot.node_id;
		record->identity.origin_owner_incarnation
			= cluster_membership_get_last_admitted_incarnation(slot.node_id);
		if (record->identity.origin_owner_incarnation == 0
			|| !read_thread_claim_fields((uint16)(i + 1), slot.node_id, &record->identity)) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
		record->root_flags
			= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
			  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
		/* RF-ROOT P9 verification (contract): every snapshot_validate field
		 * must be backed — lineage/publish/kind/tli/CRC.  The record
		 * CRCs (checkpoint + tail-last) come from the WAL stream via
		 * migration_wal_scan (step b-2); the fold-recovery bounds are
		 * new-mint values (no recovery progress yet). */
		record->identity.root_lineage_seq = 1;
		record->root_publish_seq = 1;
		record->checkpoint_lower_lsn = slot.checkpoint_redo_lsn;
		record->checkpoint_tli = slot.tli;
		record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
		record->validated_tail_lsn_exclusive = slot.highest_lsn;
		record->tail_tli = slot.tli;
		record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
		record->recovered_through_lsn_exclusive = slot.checkpoint_redo_lsn;
		record->recovered_tli = slot.tli;
		if (!migration_wal_scan((uint16)(i + 1), slot.tli, slot.checkpoint_redo_lsn,
								slot.highest_lsn, &record->checkpoint_record_crc32c,
								&record->tail_last_record_lsn, &record->tail_last_record_crc32c)) {
			pfree(second);
			pfree(first);
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		/* RF-ROOT P9 verification redo part 3: the scan fills the tail-last
		 * record fields; snapshot_validate requires the matching
		 * TAIL_LAST_RECORD_VALID flag whenever a tail-last record exists
		 * (and forbids the flag when the fields are empty — the
		 * checkpoint-at-write-pos degenerate case, where the scan leaves
		 * both zero).  Without the flag the live first-open image failed
		 * migration_image_validate (RANGE_INVALID) and create_prepared
		 * refused the round. */
		if (record->tail_last_record_lsn != 0 && record->tail_last_record_crc32c != 0)
			record->root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
		record->published_at_usec = image.created_at_usec;
		record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
		assigned++;
	}
	image.assigned_record_count = assigned;
	pfree(second);
	pfree(first);
	*out = image;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_create_prepared(const ClusterControlRootMigrationImage *image,
									 const ClusterControlRootMigrationRoundV1 *round,
									 ClusterControlRootFileToken *out_token)
{
	ControlRootImage *root;
	ControlRootImage *primary;
	ControlRootImage *bak;
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;
	uint8 round_bytes[80];
	uint8 current_uuid[16];
	uint16 i;

	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (!encode_round(round, round_bytes) || !migration_image_validate(image, round))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* A valid migration image describes bytes, not authority to create the
	 * cluster root.  Until the R4 cutover owner binds its exact proof, reject
	 * before storage-contract probing, CF acquisition, or file publication. */
	if (!cluster_control_root_create_authority_current_v1(image, round))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = storage_contract_check(image->storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!current_storage_uuid(current_uuid) || memcmp(current_uuid, image->storage_uuid, 16) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;

	root = palloc(sizeof(*root));
	memset(root, 0, sizeof(*root));
	root->header.file_txn_seq = 1;
	root->header.system_identifier = image->system_identifier;
	memcpy(root->header.storage_uuid, image->storage_uuid, 16);
	memcpy(root->header.authority_uuid, image->authority_uuid, 16);
	root->header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED;
	root->header.created_at_usec = image->created_at_usec;
	root->header.published_at_usec = GetCurrentTimestamp();
	root->header.migration_prepare_generation = round->prepare_generation;
	root->header.migration_transition_epoch = round->transition_epoch;
	root->header.source_feature_bitmap = round->source_feature_bitmap;
	root->header.target_feature_bitmap = round->target_feature_bitmap;
	if (!control_root_sha256(round_bytes, sizeof(round_bytes), root->header.migration_round_sha256))
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	else
		result = read_source_wal_state(image->records, NULL, root->header.source_wal_state_sha256,
									   round);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; i++) {
			if (image->records[i].identity.system_identifier == 0)
				continue;
			root->records[i] = image->records[i];
			root->records[i].published_at_usec = GetCurrentTimestamp();
			root->records[i].lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
			encode_record(root->bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES
							  + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
						  &root->records[i], round->coordinator_incarnation,
						  round->coordinator_node_id,
						  CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT);
			root->present[i] = true;
		}
		encode_header(root);
		if (!publish_initial_image(root))
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		primary = palloc(sizeof(*primary));
		bak = palloc(sizeof(*bak));
		result = read_canonical_pair(primary, bak);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| memcmp(primary->bytes, root->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0
			|| memcmp(bak->bytes, root->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
			result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
		else {
			make_file_token(primary, &token);
			if (token.file_txn_seq == 0)
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		}
		pfree(bak);
		pfree(primary);
	}
	pfree(root);
	result = release_cf(ExclusiveLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && out_token != NULL)
		*out_token = token;
	return result;
}

ClusterControlRootResult
cluster_control_root_activate_prepared(const ClusterControlRootFileToken *expected_token,
									   const uint8 expected_round_sha256[32],
									   const ClusterControlRootMigrationRoundV1 *round,
									   ClusterControlRootFileToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ControlRootImage *updated;
	ClusterControlRootFileToken actual;
	ClusterControlRootFileToken token;
	ClusterControlRootResult result;
	uint8 source_hash[PG_SHA256_DIGEST_LENGTH];

	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (expected_token == NULL || expected_round_sha256 == NULL || round == NULL
		|| expected_token->file_txn_seq == 0
		|| expected_token->activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED
		|| expected_token->format_version != CONTROL_ROOT_FORMAT_VERSION
		|| expected_token->record_count != CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| bytes_are_zero(expected_round_sha256, PG_SHA256_DIGEST_LENGTH))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* The PREPARED token and round hash establish freshness only.  Activation
	 * also requires the cutover owner's separately bound authority (the round
	 * carries the coordinator identity + ACK-binding fields for the proof). */
	if (!cluster_control_root_activate_authority_current_v1(expected_token, expected_round_sha256,
															round))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	updated = palloc(sizeof(*updated));
	result = read_canonical_pair(primary, bak);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		make_file_token(primary, &actual);
		if (!file_token_equal(expected_token, &actual))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else if (memcmp(primary->header.migration_round_sha256, expected_round_sha256,
						PG_SHA256_DIGEST_LENGTH)
				 != 0)
			result = CLUSTER_CONTROL_ROOT_MIGRATION_ROUND_MISMATCH;
		else
			result = read_source_wal_state(
				primary->records, primary->header.source_wal_state_sha256, source_hash, round);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		memcpy(updated, primary, sizeof(*updated));
		if (updated->header.file_txn_seq == UINT64_MAX)
			result = CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
		else {
			updated->header.file_txn_seq++;
			updated->header.activation_state = CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE;
			updated->header.published_at_usec = GetCurrentTimestamp();
			encode_header(updated);
			if (!publish_updated_image(primary, updated))
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			else {
				ControlRootImage *readback = palloc(sizeof(*readback));
				uint8 current_uuid[16];
				char path[MAXPGPATH];

				if (!current_storage_uuid(current_uuid)
					|| !build_control_path(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH))
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				else
					result = read_one_image(path, current_uuid, GetSystemIdentifier(), readback);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					&& memcmp(readback->bytes, updated->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES)
						   != 0)
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
					make_file_token(readback, &token);
				pfree(readback);
			}
		}
	}
	pfree(updated);
	pfree(bak);
	pfree(primary);
	result = release_cf(ExclusiveLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && out_token != NULL)
		*out_token = token;
	return result;
}

static uint64
reason_mask(ClusterControlRootPublishReason reason)
{
	switch (reason) {
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN:
	case CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN:
		return UINT64_C(0x3b);
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE:
		return UINT64_C(0x39);
	case CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_DUTY_OPEN:
		return UINT64_C(0xb1);
	case CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED:
		return CLUSTER_CONTROL_ROOT_PATCH_TAIL;
	case CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_PROGRESS:
		return CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS;
	case CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE:
		return UINT64_C(0x21);
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE:
		return CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	case CLUSTER_CONTROL_ROOT_PUBLISH_CONSERVATIVE_BOUND:
		return CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND;
	case CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE:
		return UINT64_C(0x38);
	case CLUSTER_CONTROL_ROOT_PUBLISH_FPW_STICKY:
		return CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY;
	default:
		return 0;
	}
}

static bool
patch_shape_valid(const ClusterControlRootPatch *patch, ClusterControlRootPublishReason reason)
{
	ClusterControlRootSnapshot allowed;
	uint64 mask = reason_mask(reason);

	if (patch == NULL || mask == 0 || patch->mask != mask || patch->reserved20 != 0
		|| patch->reserved24 != 0 || patch->expected_lifecycle < CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
		|| patch->expected_lifecycle > CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED
		|| (patch->expected_flags_mask & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0
		|| (patch->expected_flags_value & ~patch->expected_flags_mask) != 0
		|| !snapshot_reserved_zero(&patch->desired)
		|| (patch->desired.root_flags & ~CLUSTER_CONTROL_ROOT_FLAGS_V1) != 0)
		return false;
	if (reason == CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN
		&& (patch->expected_lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
			|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			|| patch->desired.identity.origin_owner_incarnation == 0
			|| patch->desired.identity.root_lineage_seq == 0))
		return false;
	/*
	 * RF-ROOT P6 (STOP-01 frozen THREAD_OPEN / THREAD_CLEAN_CLOSE
	 * transitions — Oracle clean-close/open mainline):  a clean shutdown
	 * closes the redo thread (OPEN -> CLOSED, owner lineage unchanged) and
	 * a normal restart reopens it (CLOSED -> OPEN with the fresh boot
	 * incarnation and lineage+1).  Crash / immediate-stop paths never write
	 * CLOSED and therefore never reach THREAD_OPEN (expected-lifecycle
	 * mismatch fails the CAS);  they stay on the survivor-driven
	 * failure-recovery FSM instead.
	 */
	if (reason == CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE
		&& (patch->expected_lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED))
		return false;
	if (reason == CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN
		&& (patch->expected_lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			|| patch->desired.identity.origin_owner_incarnation == 0
			|| patch->desired.identity.root_lineage_seq == 0))
		return false;
	memset(&allowed, 0, sizeof(allowed));
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE) != 0)
		allowed.lifecycle = patch->desired.lifecycle;
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE) != 0) {
		allowed.identity.origin_owner_incarnation
			= patch->desired.identity.origin_owner_incarnation;
		allowed.identity.root_lineage_seq = patch->desired.identity.root_lineage_seq;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT) != 0) {
		allowed.checkpoint_tli = patch->desired.checkpoint_tli;
		allowed.checkpoint_source_kind = patch->desired.checkpoint_source_kind;
		allowed.checkpoint_lower_lsn = patch->desired.checkpoint_lower_lsn;
		allowed.checkpoint_record_crc32c = patch->desired.checkpoint_record_crc32c;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_TAIL) != 0) {
		allowed.tail_tli = patch->desired.tail_tli;
		allowed.tail_validation_kind = patch->desired.tail_validation_kind;
		allowed.validated_tail_lsn_exclusive = patch->desired.validated_tail_lsn_exclusive;
		allowed.tail_last_record_lsn = patch->desired.tail_last_record_lsn;
		allowed.tail_last_record_crc32c = patch->desired.tail_last_record_crc32c;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS) != 0) {
		allowed.recovered_tli = patch->desired.recovered_tli;
		allowed.recovered_through_lsn_exclusive = patch->desired.recovered_through_lsn_exclusive;
		allowed.recovered_last_record_lsn = patch->desired.recovered_last_record_lsn;
		allowed.recovered_last_record_crc32c = patch->desired.recovered_last_record_crc32c;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND) != 0) {
		allowed.conservative_bound_kind = patch->desired.conservative_bound_kind;
		allowed.conservative_commit_scn = patch->desired.conservative_commit_scn;
	}
	if ((mask
		 & (CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT | CLUSTER_CONTROL_ROOT_PATCH_TAIL
			| CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS | CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY
			| CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND))
		!= 0)
		allowed.root_flags = patch->desired.root_flags;
	return memcmp(&allowed, &patch->desired, sizeof(allowed)) == 0;
}

static void
merge_flag_group(uint32 *flags, uint32 desired, uint32 group)
{
	*flags = (*flags & ~group) | (desired & group);
}

static bool
apply_patch(ClusterControlRootSnapshot *snapshot, const ClusterControlRootPatch *patch)
{
	uint64 mask = patch->mask;
	uint32 old_flags = snapshot->root_flags;

	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE) != 0)
		snapshot->lifecycle = patch->desired.lifecycle;
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_OWNER_LINEAGE) != 0) {
		snapshot->identity.origin_owner_incarnation
			= patch->desired.identity.origin_owner_incarnation;
		snapshot->identity.root_lineage_seq = patch->desired.identity.root_lineage_seq;
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CHECKPOINT) != 0) {
		snapshot->checkpoint_tli = patch->desired.checkpoint_tli;
		snapshot->checkpoint_source_kind = patch->desired.checkpoint_source_kind;
		snapshot->checkpoint_lower_lsn = patch->desired.checkpoint_lower_lsn;
		snapshot->checkpoint_record_crc32c = patch->desired.checkpoint_record_crc32c;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
						 CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_TAIL) != 0) {
		snapshot->tail_tli = patch->desired.tail_tli;
		snapshot->tail_validation_kind = patch->desired.tail_validation_kind;
		snapshot->validated_tail_lsn_exclusive = patch->desired.validated_tail_lsn_exclusive;
		snapshot->tail_last_record_lsn = patch->desired.tail_last_record_lsn;
		snapshot->tail_last_record_crc32c = patch->desired.tail_last_record_crc32c;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
						 CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
							 | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_RECOVERY_PROGRESS) != 0) {
		snapshot->recovered_tli = patch->desired.recovered_tli;
		snapshot->recovered_through_lsn_exclusive = patch->desired.recovered_through_lsn_exclusive;
		snapshot->recovered_last_record_lsn = patch->desired.recovered_last_record_lsn;
		snapshot->recovered_last_record_crc32c = patch->desired.recovered_last_record_crc32c;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
						 CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
							 | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_FPW_STICKY) != 0) {
		if ((old_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) != 0
			&& (patch->desired.root_flags & CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF) == 0)
			return false;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
						 CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF);
	}
	if ((mask & CLUSTER_CONTROL_ROOT_PATCH_CONSERVATIVE_BOUND) != 0) {
		snapshot->conservative_bound_kind = patch->desired.conservative_bound_kind;
		snapshot->conservative_commit_scn = patch->desired.conservative_commit_scn;
		merge_flag_group(&snapshot->root_flags, patch->desired.root_flags,
						 CLUSTER_CONTROL_ROOT_FLAG_CONSERVATIVE_SCN_VALID);
	}
	return true;
}

static bool
root_publish_requires_walr(ClusterControlRootPublishReason reason, bool *require_sealed_pin)
{
	*require_sealed_pin = false;
	switch (reason) {
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN:
	case CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE:
	case CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_DUTY_OPEN:
	case CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED:
	case CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN:
	case CLUSTER_CONTROL_ROOT_PUBLISH_CHECKPOINT_ADVANCE:
		return true;
	case CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE:
		*require_sealed_pin = true;
		return true;
	default:
		return false;
	}
}

ClusterControlRootResult
cluster_control_root_compare_and_publish(const ClusterControlRootReadToken *expected_token,
										 const ClusterControlRootPatch *patch,
										 ClusterControlRootPublishReason reason,
										 ClusterControlRootSnapshot *out_snapshot,
										 ClusterControlRootReadToken *out_token)
{
	ControlRootImage *primary;
	ControlRootImage *bak;
	ControlRootImage *updated;
	ClusterControlRootReadToken actual;
	ClusterControlRootReadToken token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootResult result;
	ClusterWalRootPublishGuard *walr_guard = NULL;
	ClusterWalPinResult walr_result;
	ClusterWalrReleaseResult walr_release_result;
	uint16 thread_id;
	bool require_sealed_pin;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (expected_token == NULL || expected_token->source != CONTROL_ROOT_SOURCE_PRIMARY
		|| expected_token->origin_thread_id == 0
		|| expected_token->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| expected_token->reserved20 != 0 || expected_token->reserved32 != 0
		|| !patch_shape_valid(patch, reason))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* RF-ROOT P5: the byte-exact CAS token proves root freshness, not caller
	 * authority.  Consume the backend-private authority bound by the owning
	 * publisher before CF acquisition or any file I/O. */
	if (!cluster_control_root_publish_authority_current_v1(expected_token, patch, reason))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	thread_id = expected_token->origin_thread_id;
	if (root_publish_requires_walr(reason, &require_sealed_pin)) {
		walr_result = cluster_wal_retention_root_publish_begin_exact(
			expected_token, require_sealed_pin, &walr_guard);
		if (walr_result == CLUSTER_WAL_PIN_INVALID)
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		if (walr_result == CLUSTER_WAL_PIN_STALE)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (walr_result != CLUSTER_WAL_PIN_OK)
			return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	}
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto release_walr;
	if (!acquire_clusterwide_cf(ExclusiveLock)) {
		result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		goto release_walr;
	}
	primary = palloc(sizeof(*primary));
	bak = palloc(sizeof(*bak));
	updated = palloc(sizeof(*updated));
	result = read_canonical_pair(primary, bak);
	if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		&& !primary->present[thread_id - 1])
		result = CLUSTER_CONTROL_ROOT_ABSENT;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		make_read_token(primary, thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &actual);
		if (!read_token_equal(expected_token, &actual))
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else if (primary->records[thread_id - 1].lifecycle != patch->expected_lifecycle
				 || (primary->records[thread_id - 1].root_flags & patch->expected_flags_mask)
						!= patch->expected_flags_value)
			result = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
		else if ((reason == CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN
				  || reason == CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN)
				 && (primary->records[thread_id - 1].identity.root_lineage_seq == UINT64_MAX
					 || patch->desired.identity.root_lineage_seq
							!= primary->records[thread_id - 1].identity.root_lineage_seq + 1
					 || patch->desired.identity.origin_owner_incarnation
							<= primary->records[thread_id - 1].identity.origin_owner_incarnation))
			result = CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		memcpy(updated, primary, sizeof(*updated));
		snapshot = updated->records[thread_id - 1];
		if (updated->header.file_txn_seq == UINT64_MAX || snapshot.root_publish_seq == UINT64_MAX)
			result = CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
		else if (!apply_patch(&snapshot, patch))
			result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		else {
			snapshot.root_publish_seq++;
			snapshot.published_at_usec = GetCurrentTimestamp();
			snapshot.lifecycle_reason = reason;
			result = snapshot_validate(&snapshot, thread_id, updated->header.system_identifier,
									   updated->header.storage_uuid, updated->header.authority_uuid,
									   false);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			updated->records[thread_id - 1] = snapshot;
			encode_record(updated->bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES
							  + (size_t)(thread_id - 1) * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
						  &snapshot, snapshot.identity.origin_owner_incarnation,
						  (uint32)cluster_node_id, reason);
			updated->header.file_txn_seq++;
			updated->header.published_at_usec = snapshot.published_at_usec;
			encode_header(updated);
			if (!publish_updated_image(primary, updated))
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			else {
				ControlRootImage *readback = palloc(sizeof(*readback));
				uint8 current_uuid[16];
				char path[MAXPGPATH];

				if (!current_storage_uuid(current_uuid)
					|| !build_control_path(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH))
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				else
					result = read_one_image(path, current_uuid, GetSystemIdentifier(), readback);
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					&& memcmp(readback->bytes, updated->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES)
						   != 0)
					result = CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
					snapshot = readback->records[thread_id - 1];
					make_read_token(readback, thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &token);
				}
				pfree(readback);
			}
		}
	}
	pfree(updated);
	pfree(bak);
	pfree(primary);
	result = release_cf(ExclusiveLock, result);

release_walr:
	if (walr_guard != NULL) {
		walr_release_result = cluster_wal_retention_root_publish_end(&walr_guard);
		if (walr_release_result != CLUSTER_WALR_RELEASE_CONFIRMED)
			result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (out_snapshot != NULL)
			*out_snapshot = snapshot;
		if (out_token != NULL)
			*out_token = token;
	}
	return result;
}

ClusterControlRootResult
cluster_control_root_revalidate(const ClusterControlRootReadToken *token,
								const ClusterControlRootIdentity *expected_identity,
								ClusterControlRootSnapshot *out_snapshot)
{
	ClusterControlRootReadToken fresh;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootResult result;

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (token == NULL || expected_identity == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result
		= cluster_control_root_read_canonical(token->origin_thread_id, expected_identity,
											  CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot, &fresh);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!read_token_equal(token, &fresh))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (out_snapshot != NULL)
		*out_snapshot = snapshot;
	return result;
}

/*
 * cluster_control_root_round_sha256 -- RF-ROOT P7 (contract): the round
 * wire-encoded sha256 (same bytes create_prepared stores in the root header
 * migration_round_sha256).  The cutover driver needs it to stage the seam.
 */
bool
cluster_control_root_round_sha256(const ClusterControlRootMigrationRoundV1 *round,
								  uint8 out_sha[PG_SHA256_DIGEST_LENGTH])
{
	uint8 round_bytes[80];

	if (round == NULL || out_sha == NULL || !encode_round(round, round_bytes))
		return false;
	return control_root_sha256(round_bytes, sizeof(round_bytes), out_sha);
}

ClusterControlRootResult
cluster_control_root_discard_inactive(const ClusterControlRootFileToken *expected_token,
									  const uint8 expected_round_sha256[32])
{
	/* P1 deliberately exposes no cutover path.  The R4 abort callback and its
	 * both-admissions-closed proof arrive at P2-P5; until then this API is
	 * fail-closed before CF or storage I/O. */
	(void)expected_token;
	(void)expected_round_sha256;
	return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
}
