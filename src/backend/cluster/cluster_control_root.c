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
#include "cluster/cluster_clean_leave.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_conf.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_grd.h"
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
#include "cluster/cluster_wal_source.h"
#include "cluster/cluster_wal_writer.h"
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
#include "storage/latch.h"
#include "utils/timestamp.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#define CONTROL_ROOT_HEADER_MAGIC "PGCH"
#define CONTROL_ROOT_RECORD_MAGIC "PGRT"
#define CONTROL_ROOT_FORMAT_VERSION UINT16_C(1)
#define CONTROL_ROOT_ENDIAN_TAG UINT32_C(0x01020304)
#define CONTROL_ROOT_READER_VERSION UINT16_C(1)
#define CONTROL_ROOT_WRITER_VERSION UINT16_C(1)
#define CONTROL_ROOT_HEADER_VERSION_V2 UINT16_C(2)
#define CONTROL_ROOT_RECORD_VERSION_V2 UINT16_C(2)
#define CONTROL_ROOT_HEADER_VERSION_V3 UINT16_C(3)
#define CONTROL_ROOT_RECORD_VERSION_V3 UINT16_C(3)
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

static bool publish_updated_image(const ControlRootImage *old_image,
								  const ControlRootImage *new_image);

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

/* A failed startup claim is the next incarnation of the same origin.  Its
 * owner incarnation, lineage, claim timestamp and claim CRC are deliberately
 * different from the currently selected root; only the immutable database,
 * authority, thread and node namespace may be shared across the rejoin proof.
 */
static bool
rejoin_terminal_namespace_equal(const ClusterControlRootIdentity *left,
								const ClusterControlRootIdentity *right)
{
	return left != NULL && right != NULL && left->system_identifier == right->system_identifier
		   && memcmp(left->storage_uuid, right->storage_uuid, 16) == 0
		   && memcmp(left->authority_uuid, right->authority_uuid, 16) == 0
		   && left->origin_thread_id == right->origin_thread_id
		   && left->origin_node_id == right->origin_node_id && left->reserved42 == 0
		   && right->reserved42 == 0 && left->reserved60 == 0 && right->reserved60 == 0;
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
		|| common->database_state > CLUSTER_CONTROL_ROOT_DATABASE_CLOSED)
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
					  const ControlRootRecordRefsV2 *refs, const ControlRootStartupRefV3 *startup)
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
	if (version == CONTROL_ROOT_RECORD_VERSION_V2 || version == CONTROL_ROOT_RECORD_VERSION_V3) {
		write_u64_le(dst + 216, refs->history_generation);
		memcpy(dst + 224, refs->history_sha256, 32);
		write_u64_le(dst + 256, refs->anchor_generation);
		memcpy(dst + 264, refs->anchor_sha256, 32);
		memcpy(dst + 296, refs->claim_sha256, 32);
	}
	if (version == CONTROL_ROOT_RECORD_VERSION_V3) {
		write_u64_le(dst + 328, startup->generation);
		memcpy(dst + 336, startup->sha256, 32);
	}
	write_u32_le(dst + CONTROL_ROOT_RECORD_CRC_OFFSET,
				 control_root_crc(dst, CONTROL_ROOT_RECORD_CRC_OFFSET));
}

static void
encode_record(uint8 *dst, const ClusterControlRootSnapshot *snapshot, uint64 publisher_incarnation,
			  uint32 publisher_node, ClusterControlRootPublishReason reason)
{
	encode_record_version(dst, snapshot, publisher_incarnation, publisher_node, reason,
						  CONTROL_ROOT_FORMAT_VERSION, NULL, NULL);
}

static ClusterControlRootResult
decode_record(const uint8 *src, uint16 expected_thread, const ControlRootHeader *header,
			  ClusterControlRootSnapshot *snapshot, uint32 *crc_out, uint16 expected_version,
			  ControlRootRecordRefsV2 *refs, ControlRootStartupRefV3 *startup)
{
	ClusterControlRootResult result;
	uint64 publisher_incarnation;
	uint32 publisher_node;
	uint32 reason;
	uint32 stored_crc;
	bool extended = expected_version == CONTROL_ROOT_RECORD_VERSION_V2
					|| expected_version == CONTROL_ROOT_RECORD_VERSION_V3;
	size_t reserved_start = expected_version == CONTROL_ROOT_RECORD_VERSION_V3 ? 368
							: extended										   ? 328
																			   : 216;

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
		|| !bytes_are_zero(src + reserved_start, 504 - reserved_start)
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
							   header->storage_uuid, header->authority_uuid, extended);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (extended) {
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
	if (expected_version == CONTROL_ROOT_RECORD_VERSION_V3) {
		startup->generation = read_u64_le(src + 328);
		memcpy(startup->sha256, src + 336, 32);
		if ((startup->generation == 0) != bytes_are_zero(startup->sha256, 32))
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
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

static ClusterControlRootResult
history_terminal_set_validate(const ControlRootImage *root, uint32 node,
							  const ClusterWalHistoryImage *history)
{
	if (history->count > CLUSTER_WAL_HISTORY_MAX_RECORDS
		|| history->terminal_count > CLUSTER_WAL_HISTORY_MAX_RECORDS - history->count)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	for (uint32 i = 0; i < history->terminal_count; i++) {
		const ClusterWalTerminalRef *ref = &history->terminals[i];
		if (ref->incarnation == 0 || ref->generation == 0 || bytes_are_zero(ref->sha256, 32)
			|| (i > 0 && ref->incarnation <= history->terminals[i - 1].incarnation))
			return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		if (ref->incarnation == root->records[node].identity.origin_owner_incarnation)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		for (uint32 j = 0; j < history->count; j++)
			if (ref->incarnation == history->records[j].snapshot.identity.origin_owner_incarnation)
				return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
decode_history_version(const uint8 *bytes, size_t len, const ControlRootImage *root,
					   uint32 origin_node, ClusterWalHistoryImage *out, uint16 version)
{
	ClusterControlRootResult result;
	uint8 hash[32];
	uint32 count, terminals = 0;
	uint16 manifest_version, header_bytes;
	uint64 body_bytes;
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
	if (root->header.format_version != version)
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
	manifest_version = read_u16_le(bytes + 4);
	if (manifest_version != 1 && !(manifest_version == 2 && version == 3))
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	header_bytes = manifest_version == 1 ? CLUSTER_WAL_HISTORY_HEADER_BYTES
										 : CLUSTER_WAL_HISTORY_V2_HEADER_BYTES;
	if (len < header_bytes + 4)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (manifest_version == 2) {
		terminals = read_u32_le(bytes + 64);
		if (!bytes_are_zero(bytes + 72, 24))
			return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
		if (terminals == 0 || terminals > CLUSTER_WAL_HISTORY_MAX_RECORDS
			|| read_u32_le(bytes + 68) != CLUSTER_WAL_TERMINAL_REF_BYTES)
			return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	}
	count = read_u32_le(bytes + 8);
	body_bytes = (uint64)count * CLUSTER_CONTROL_ROOT_RECORD_BYTES
				 + (uint64)terminals * CLUSTER_WAL_TERMINAL_REF_BYTES;
	if (read_u16_le(bytes + 6) != header_bytes
		|| count > CLUSTER_WAL_HISTORY_MAX_RECORDS - terminals
		|| read_u32_le(bytes + 12) != CLUSTER_CONTROL_ROOT_RECORD_BYTES
		|| read_u64_le(bytes + 16) != body_bytes || len != header_bytes + body_bytes + 4)
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
		const uint8 *src = bytes + header_bytes + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES;
		ClusterWalHistoryRecord *record = &out->records[i];
		uint64 incarnation;

		result = decode_record(src, origin_node + 1, &root->header, &record->snapshot,
							   &record->record_crc32c, CONTROL_ROOT_RECORD_VERSION_V2,
							   &record->refs, NULL);
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
	out->terminal_count = terminals;
	for (uint32 i = 0; i < terminals; i++) {
		const uint8 *src = bytes + header_bytes + (size_t)count * CLUSTER_CONTROL_ROOT_RECORD_BYTES
						   + (size_t)i * CLUSTER_WAL_TERMINAL_REF_BYTES;
		out->terminals[i].incarnation = read_u64_le(src);
		out->terminals[i].generation = read_u64_le(src + 8);
		memcpy(out->terminals[i].sha256, src + 16, 32);
	}
	result = history_terminal_set_validate(root, origin_node, out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto refused;
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

static ClusterControlRootResult
encode_history_version(const ControlRootImage *root, uint32 origin_node,
					   const ClusterWalHistoryImage *history,
					   uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], size_t *length, uint16 version)
{
	ClusterControlRootResult result;
	size_t len, header_bytes;
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
	if (root->header.format_version != version)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[origin_node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (history->terminal_count != 0 && version != 3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	result = history_terminal_set_validate(root, origin_node, history);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
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
	header_bytes = history->terminal_count == 0 ? CLUSTER_WAL_HISTORY_HEADER_BYTES
												: CLUSTER_WAL_HISTORY_V2_HEADER_BYTES;
	len = header_bytes + (size_t)history->count * CLUSTER_CONTROL_ROOT_RECORD_BYTES
		  + (size_t)history->terminal_count * CLUSTER_WAL_TERMINAL_REF_BYTES + 4;
	memcpy(bytes, "PGWH", 4);
	write_u16_le(bytes + 4, history->terminal_count == 0 ? 1 : 2);
	write_u16_le(bytes + 6, header_bytes);
	write_u32_le(bytes + 8, history->count);
	write_u32_le(bytes + 12, CLUSTER_CONTROL_ROOT_RECORD_BYTES);
	write_u64_le(bytes + 16, len - header_bytes - 4);
	write_u64_le(bytes + 24, root->header.system_identifier);
	memcpy(bytes + 32, root->header.storage_uuid, 16);
	memcpy(bytes + 48, root->header.authority_uuid, 16);
	if (history->terminal_count != 0) {
		write_u32_le(bytes + 64, history->terminal_count);
		write_u32_le(bytes + 68, CLUSTER_WAL_TERMINAL_REF_BYTES);
	}
	for (uint32 i = 0; i < history->count; i++) {
		const ClusterWalHistoryRecord *r = &history->records[i];
		encode_record_version(bytes + header_bytes + (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
							  &r->snapshot, r->publisher_incarnation, r->publisher_node,
							  (ClusterControlRootPublishReason)r->snapshot.lifecycle_reason,
							  CONTROL_ROOT_RECORD_VERSION_V2, &r->refs, NULL);
	}
	for (uint32 i = 0; i < history->terminal_count; i++) {
		const ClusterWalTerminalRef *ref = &history->terminals[i];
		uint8 *dst = bytes + header_bytes
					 + (size_t)history->count * CLUSTER_CONTROL_ROOT_RECORD_BYTES
					 + (size_t)i * CLUSTER_WAL_TERMINAL_REF_BYTES;
		write_u64_le(dst, ref->incarnation);
		write_u64_le(dst + 8, ref->generation);
		memcpy(dst + 16, ref->sha256, 32);
	}
	write_u32_le(bytes + len - 4, control_root_crc(bytes, len - 4));
	*length = len;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v2_history_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalHistoryImage *out)
{
	return decode_history_version(bytes, len, root, origin_node, out,
								  CONTROL_ROOT_HEADER_VERSION_V2);
}

ClusterControlRootResult
cluster_control_root_v2_history_encode(const ControlRootImage *root, uint32 origin_node,
									   const ClusterWalHistoryImage *history,
									   uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], size_t *length)
{
	return encode_history_version(root, origin_node, history, bytes, length,
								  CONTROL_ROOT_HEADER_VERSION_V2);
}

ClusterControlRootResult
cluster_control_root_v3_history_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalHistoryImage *out)
{
	return decode_history_version(bytes, len, root, origin_node, out,
								  CONTROL_ROOT_HEADER_VERSION_V3);
}

ClusterControlRootResult
cluster_control_root_v3_history_encode(const ControlRootImage *root, uint32 origin_node,
									   const ClusterWalHistoryImage *history,
									   uint8 bytes[CLUSTER_WAL_HISTORY_MAX_BYTES], size_t *length)
{
	return encode_history_version(root, origin_node, history, bytes, length,
								  CONTROL_ROOT_HEADER_VERSION_V3);
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
	if (version == CONTROL_ROOT_HEADER_VERSION_V2 || version == CONTROL_ROOT_HEADER_VERSION_V3) {
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
startup_ref_validate(const ControlRootImage *image, uint16 node)
{
	const ControlRootStartupRefV3 *ref = &image->startup[node];
	const ControlRootCommonV2 *common = &image->header.v2;
	uint64 bit = UINT64_C(1) << (node % 64);
	uint32 lifecycle = image->records[node].lifecycle;

	if ((ref->generation == 0) != bytes_are_zero(ref->sha256, 32))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (ref->generation == 0)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	if (!image->present[node] || (common->configured[node / 64] & bit) == 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if ((common->serving[node / 64] & bit) != 0
		|| (lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			&& lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE)
		|| common->database_state >= CLUSTER_CONTROL_ROOT_DATABASE_CLOSED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
decode_image_version(ControlRootImage *image, const uint8 current_uuid[16], uint64 current_sysid,
					 uint16 expected_version)
{
	const uint8 *src = image->bytes;
	uint32 stored_crc;
	uint16 i;
	bool extended = expected_version == CONTROL_ROOT_HEADER_VERSION_V2
					|| expected_version == CONTROL_ROOT_HEADER_VERSION_V3;

	memset(&image->header, 0, sizeof(image->header));
	memset(image->records, 0, sizeof(image->records));
	memset(image->refs, 0, sizeof(image->refs));
	memset(image->startup, 0, sizeof(image->startup));
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
	if (!bytes_are_zero(src + (extended ? 376 : 196), extended ? 128 : 308)
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
	if (extended) {
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
			expected_version, &image->refs[i], &image->startup[i]);

		if (result == CLUSTER_CONTROL_ROOT_ABSENT)
			continue;
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		image->present[i] = true;
		image->publisher_incarnation[i] = read_u64_le(record + 144);
		image->publisher_node[i] = read_u32_le(record + 152);
		if (expected_version == CONTROL_ROOT_HEADER_VERSION_V3) {
			result = startup_ref_validate(image, i);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
		}
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: these codecs validate representation, not proof of publication.
 * Existing v1 file APIs never call them. Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
decode_extended_image(const uint8 *bytes, size_t len, const uint8 storage_uuid[16],
					  uint64 system_identifier, ControlRootImage *out, uint16 version)
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
	result = decode_image_version(out, expected_uuid, system_identifier, version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

static ClusterControlRootResult
encode_extended_image(ControlRootImage *image, uint16 version)
{
	ClusterControlRootResult result;
	uint8 expected_uuid[16];
	uint64 system_identifier;
	uint16 i;

	if (image == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(image->bytes, 0, sizeof(image->bytes));
	if (image->header.format_version != version)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (version != CONTROL_ROOT_HEADER_VERSION_V3
		&& !bytes_are_zero(image->startup, sizeof(image->startup)))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (bytes_are_zero(image->header.storage_uuid, 16))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = common_v2_validate(&image->header.v2);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++i) {
		if (!image->present[i]) {
			if (!bytes_are_zero(&image->records[i], sizeof(image->records[i]))
				|| !bytes_are_zero(&image->refs[i], sizeof(image->refs[i]))
				|| !bytes_are_zero(&image->startup[i], sizeof(image->startup[i]))
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
		if (version == CONTROL_ROOT_HEADER_VERSION_V3) {
			result = startup_ref_validate(image, i);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
		}
	}
	for (i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++i)
		if (image->present[i])
			encode_record_version(
				image->bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES
					+ (size_t)i * CLUSTER_CONTROL_ROOT_RECORD_BYTES,
				&image->records[i], image->publisher_incarnation[i], image->publisher_node[i],
				(ClusterControlRootPublishReason)image->records[i].lifecycle_reason, version,
				&image->refs[i], &image->startup[i]);
	encode_header_version(image, version);
	memcpy(expected_uuid, image->header.storage_uuid, sizeof(expected_uuid));
	system_identifier = image->header.system_identifier;
	result = decode_image_version(image, expected_uuid, system_identifier, version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(image->bytes, 0, sizeof(image->bytes));
	return result;
}

ClusterControlRootResult
cluster_control_root_v2_decode(const uint8 *bytes, size_t len, const uint8 storage_uuid[16],
							   uint64 system_identifier, ControlRootImage *out)
{
	return decode_extended_image(bytes, len, storage_uuid, system_identifier, out,
								 CONTROL_ROOT_HEADER_VERSION_V2);
}

ClusterControlRootResult
cluster_control_root_v3_decode(const uint8 *bytes, size_t len, const uint8 storage_uuid[16],
							   uint64 system_identifier, ControlRootImage *out)
{
	return decode_extended_image(bytes, len, storage_uuid, system_identifier, out,
								 CONTROL_ROOT_HEADER_VERSION_V3);
}

ClusterControlRootResult
cluster_control_root_v2_encode(ControlRootImage *image)
{
	return encode_extended_image(image, CONTROL_ROOT_HEADER_VERSION_V2);
}

ClusterControlRootResult
cluster_control_root_v3_encode(ControlRootImage *image)
{
	return encode_extended_image(image, CONTROL_ROOT_HEADER_VERSION_V3);
}

/* PGRAC: immutable pending input, not proof of isolation, durable native
 * initialization or permission to serve. Author: SqlRush <sqlrush@gmail.com> */
static bool
startup_retained_predecessor_matches(const ClusterWalHistoryRecord *input,
									 const ClusterControlRootSnapshot *snapshot,
									 const ControlRootRecordRefsV2 *refs,
									 uint64 publisher_incarnation, uint32 publisher_node)
{
	/* Only the enclosing history reference changes when flattening a set.
	 * Every actual checkpoint/claim/anchor identity remains exact. */
	return memcmp(&input->snapshot, snapshot, sizeof(*snapshot)) == 0
		   && input->publisher_incarnation == publisher_incarnation
		   && input->publisher_node == publisher_node
		   && input->refs.anchor_generation == refs->anchor_generation
		   && memcmp(input->refs.anchor_sha256, refs->anchor_sha256, 32) == 0
		   && memcmp(input->refs.claim_sha256, refs->claim_sha256, 32) == 0;
}

static bool
startup_predecessor_in_union(const ClusterWalHistoryRecord *input, const ControlRootImage *root,
							 uint32 node, const ClusterWalHistoryImage *history)
{
	if (history->count > CLUSTER_WAL_HISTORY_MAX_RECORDS)
		return false;
	if (startup_retained_predecessor_matches(input, &root->records[node], &root->refs[node],
											 root->publisher_incarnation[node],
											 root->publisher_node[node]))
		return true;
	for (uint32 i = 0; i < history->count; i++) {
		const ClusterWalHistoryRecord *r = &history->records[i];
		if (startup_retained_predecessor_matches(input, &r->snapshot, &r->refs,
												 r->publisher_incarnation, r->publisher_node))
			return true;
	}
	return false;
}

static ClusterControlRootResult
startup_decode_fields(const uint8 *bytes, const ControlRootImage *root, uint32 node,
					  const ControlRootStartupRefV3 *selected,
					  const ClusterWalHistoryImage *retained, ClusterWalStartupImage *out)
{
	ClusterControlRootResult result;
	ClusterWalThreadClaimRefV2 claim_ref;
	const uint8 *claim = bytes + 1280;
	uint8 hash[32];
	ClusterWalHistoryRecord *input = &out->predecessor;
	ClusterWalHistoryRecord *successor = &out->successor;
	uint64 segment_mask;

	if (root->header.format_version != CONTROL_ROOT_HEADER_VERSION_V3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[node] || selected->generation == 0)
		return CLUSTER_CONTROL_ROOT_ABSENT;
	result = common_v2_validate(&root->header.v2);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (retained == NULL) {
		result = startup_ref_validate(root, node);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (memcmp(bytes, "PGWG", 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(bytes + 4) != 1)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (read_u16_le(bytes + 6) != CLUSTER_WAL_STARTUP_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (read_u32_le(bytes + 1532) != control_root_crc(bytes, 1532))
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	if (!bytes_are_zero(bytes + 184, 72) || !bytes_are_zero(bytes + 1424, 108))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (!control_root_sha256(bytes, CLUSTER_WAL_STARTUP_BYTES, hash))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, selected->sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;

	out->phase = read_u32_le(bytes + 8);
	out->input_kind = read_u32_le(bytes + 12);
	memcpy(out->operation_uuid, bytes + 16, 16);
	out->database_incarnation = read_u64_le(bytes + 32);
	out->config_generation = read_u64_le(bytes + 40);
	out->formation_epoch = read_u64_le(bytes + 48);
	out->predecessor_file_sequence = read_u64_le(bytes + 56);
	memcpy(out->predecessor_file_sha256, bytes + 64, 32);
	out->generation = read_u64_le(bytes + 96);
	out->first_segment_lsn = read_u64_le(bytes + 104);
	out->timeline = read_u32_le(bytes + 112);
	out->segment_size = read_u32_le(bytes + 116);
	memcpy(out->predecessor_evidence_sha256, bytes + 120, 32);
	out->input_record_start = read_u64_le(bytes + 152);
	out->input_record_end = read_u64_le(bytes + 160);
	out->input_record_crc = read_u32_le(bytes + 168);
	out->input_timeline = read_u32_le(bytes + 172);
	out->sealed_input_end = read_u64_le(bytes + 176);
	if (out->phase < CLUSTER_WAL_STARTUP_RESERVED || out->phase > CLUSTER_WAL_STARTUP_DURABLE
		|| out->input_kind < CLUSTER_WAL_STARTUP_CLEAN
		|| out->input_kind > CLUSTER_WAL_STARTUP_RECOVERED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (bytes_are_zero(out->operation_uuid, 16) || bytes_are_zero(out->predecessor_file_sha256, 32)
		|| bytes_are_zero(out->predecessor_evidence_sha256, 32) || out->formation_epoch == 0
		|| out->predecessor_file_sequence == 0
		|| out->predecessor_file_sequence >= root->header.file_txn_seq
		|| out->config_generation == 0
		|| out->config_generation > root->header.v2.config_generation)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (out->database_incarnation != root->header.v2.database_incarnation
		|| out->generation != selected->generation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;

	result
		= decode_record(bytes + 256, node + 1, &root->header, &input->snapshot,
						&input->record_crc32c, CONTROL_ROOT_RECORD_VERSION_V2, &input->refs, NULL);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result == CLUSTER_CONTROL_ROOT_ABSENT ? CLUSTER_CONTROL_ROOT_RANGE_INVALID : result;
	input->publisher_incarnation = read_u64_le(bytes + 256 + 144);
	input->publisher_node = read_u32_le(bytes + 256 + 152);
	if (retained != NULL
			? !startup_predecessor_in_union(input, root, node, retained)
			: (memcmp(&input->snapshot, &root->records[node], sizeof(input->snapshot)) != 0
			   || memcmp(&input->refs, &root->refs[node], sizeof(input->refs)) != 0
			   || input->publisher_incarnation != root->publisher_incarnation[node]
			   || input->publisher_node != root->publisher_node[node]))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (input->snapshot.lifecycle
		!= (out->input_kind == CLUSTER_WAL_STARTUP_RECOVERED
				? CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
				: CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED))
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if ((input->snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0
		|| out->input_record_start
			   != (out->input_kind == CLUSTER_WAL_STARTUP_CLEAN
					   ? input->snapshot.tail_last_record_lsn
					   : input->snapshot.checkpoint_lower_lsn)
		|| out->input_record_start < input->snapshot.checkpoint_lower_lsn
		|| out->input_record_crc != input->snapshot.checkpoint_record_crc32c
		|| out->input_timeline != input->snapshot.checkpoint_tli
		|| out->timeline != out->input_timeline || input->snapshot.tail_tli != out->input_timeline
		|| out->sealed_input_end != input->snapshot.validated_tail_lsn_exclusive
		|| out->input_record_end <= out->input_record_start
		|| out->input_record_end > out->sealed_input_end)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (out->input_kind == CLUSTER_WAL_STARTUP_RECOVERED
		&& ((input->snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID) == 0
			|| input->snapshot.recovered_through_lsn_exclusive != out->sealed_input_end
			|| input->snapshot.recovered_tli != out->input_timeline))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (!IsValidWalSegSize(out->segment_size))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	segment_mask = out->segment_size - 1;
	if (out->sealed_input_end > UINT64_MAX - segment_mask
		|| out->first_segment_lsn != ((out->sealed_input_end + segment_mask) & ~segment_mask)
		|| out->first_segment_lsn > UINT64_MAX - SizeOfXLogLongPHD)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;

	/* The source identity constrains the database/origin. The new claim's CRC
 * and namespace are checked by the real claim decoder, independently of any
 * future successor record. Equality of bytes is not writer admission. */
	memset(&claim_ref, 0, sizeof(claim_ref));
	claim_ref.identity = input->snapshot.identity;
	claim_ref.identity.origin_owner_incarnation = read_u64_le(claim + 32);
	claim_ref.identity.root_lineage_seq = read_u64_le(claim + 72);
	claim_ref.identity.thread_claim_created_at = read_u64_le(claim + 80);
	claim_ref.identity.thread_claim_crc32c = read_u32_le(claim + 104);
	claim_ref.database_incarnation = out->database_incarnation;
	claim_ref.max_config_generation = out->config_generation;
	if (!control_root_sha256(claim, CLUSTER_WAL_CLAIM_V2_BYTES, claim_ref.claim_sha256))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result
		= cluster_wal_claim_v2_decode(claim, CLUSTER_WAL_CLAIM_V2_BYTES, &claim_ref, &out->claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (out->claim.config_generation != out->config_generation
		|| out->claim.identity.origin_owner_incarnation
			   == input->snapshot.identity.origin_owner_incarnation
		|| out->claim.identity.root_lineage_seq == input->snapshot.identity.root_lineage_seq)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (out->phase != CLUSTER_WAL_STARTUP_DURABLE) {
		if (!bytes_are_zero(bytes + 768, 512) || !bytes_are_zero(bytes + 1392, 32))
			return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}

	result = decode_record(bytes + 768, node + 1, &root->header, &successor->snapshot,
						   &successor->record_crc32c, CONTROL_ROOT_RECORD_VERSION_V2,
						   &successor->refs, NULL);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result == CLUSTER_CONTROL_ROOT_ABSENT ? CLUSTER_CONTROL_ROOT_RANGE_INVALID : result;
	successor->publisher_incarnation = read_u64_le(bytes + 768 + 144);
	successor->publisher_node = read_u32_le(bytes + 768 + 152);
	if (!cluster_control_root_identity_equal(&successor->snapshot.identity, &out->claim.identity)
		|| memcmp(successor->refs.claim_sha256, claim_ref.claim_sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (successor->snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (!bytes_are_zero(bytes + 1392, 32))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (successor->snapshot.checkpoint_tli != out->timeline
		|| successor->snapshot.tail_tli != out->timeline
		|| successor->snapshot.checkpoint_lower_lsn < out->first_segment_lsn + SizeOfXLogLongPHD
		|| successor->snapshot.tail_last_record_lsn != successor->snapshot.checkpoint_lower_lsn
		|| successor->snapshot.validated_tail_lsn_exclusive <= successor->snapshot.tail_last_record_lsn
		|| successor->snapshot.tail_last_record_crc32c != successor->snapshot.checkpoint_record_crc32c
		|| (successor->snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID) != 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v3_startup_decode(const uint8 *bytes, size_t len, const ControlRootImage *root,
									   uint32 origin_node, ClusterWalStartupImage *out)
{
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(root, sizeof(*root), out, sizeof(*out))
				 || (len == CLUSTER_WAL_STARTUP_BYTES
					 && history_ranges_overlap(bytes, len, out, sizeof(*out)));

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (alias || bytes == NULL || root == NULL || origin_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (len != CLUSTER_WAL_STARTUP_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	result
		= startup_decode_fields(bytes, root, origin_node, &root->startup[origin_node], NULL, out);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

ClusterControlRootResult
cluster_wal_terminal_decode(const uint8 *bytes, size_t length, const ControlRootImage *root,
							uint32 node, const ClusterWalHistoryImage *history,
							const ClusterWalTerminalRef *ref, ClusterWalTerminalImage *out)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	ClusterWalStartupObservation *observation;
	uint8 hash[32];
	bool alias = history_ranges_overlap(root, sizeof(*root), out, sizeof(*out))
				 || history_ranges_overlap(history, sizeof(*history), out, sizeof(*out))
				 || history_ranges_overlap(ref, sizeof(*ref), out, sizeof(*out))
				 || (length == CLUSTER_WAL_TERMINAL_BYTES
					 && history_ranges_overlap(bytes, length, out, sizeof(*out)));

	if (out == NULL)
		return result;
	memset(out, 0, sizeof(*out));
	if (alias || bytes == NULL || root == NULL || history == NULL || ref == NULL
		|| node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return result;
	if (root->header.format_version != 3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (length != CLUSTER_WAL_TERMINAL_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (memcmp(bytes, "PGWG", 4) != 0)
		return CLUSTER_CONTROL_ROOT_BAD_MAGIC;
	if (read_u16_le(bytes + 4) != 2)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (read_u16_le(bytes + 6) != CLUSTER_WAL_TERMINAL_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (read_u32_le(bytes + 8) != CLUSTER_WAL_INITIALIZATION_TERMINATED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (read_u32_le(bytes + 2300) != control_root_crc(bytes, 2300))
		return CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
	if (!bytes_are_zero(bytes + 108, 4) || !bytes_are_zero(bytes + 224, 32)
		|| !bytes_are_zero(bytes + 1792, 256) || !bytes_are_zero(bytes + 2120, 180))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	if (ref->incarnation == 0 || ref->generation == 0 || bytes_are_zero(ref->sha256, 32))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!control_root_sha256(bytes, length, hash))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, ref->sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	result = history_terminal_set_validate(root, node, history);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;

	out->closure_version = read_u32_le(bytes + 12);
	memcpy(out->operation_uuid, bytes + 16, 16);
	out->generation = read_u64_le(bytes + 32);
	out->database_incarnation = read_u64_le(bytes + 40);
	out->sealing_sequence = read_u64_le(bytes + 48);
	memcpy(out->sealing_sha256, bytes + 56, 32);
	out->formation_epoch = read_u64_le(bytes + 88);
	out->recoverer_incarnation = read_u64_le(bytes + 96);
	out->recoverer_node = read_u32_le(bytes + 104);
	out->ir_request_id = read_u64_le(bytes + 112);
	out->original_ref.generation = read_u64_le(bytes + 120);
	memcpy(out->original_ref.sha256, bytes + 128, 32);
	memcpy(out->isolation_sha256, bytes + 160, 32);
	memcpy(out->closure_sha256, bytes + 192, 32);
	memcpy(out->original, bytes + 256, CLUSTER_WAL_STARTUP_BYTES);
	result = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (out->closure_version != 1 || out->generation != ref->generation
		|| out->database_incarnation != root->header.v2.database_incarnation
		|| out->sealing_sequence == 0 || out->sealing_sequence > root->header.file_txn_seq
		|| bytes_are_zero(out->sealing_sha256, 32) || out->formation_epoch == 0
		|| out->recoverer_incarnation == 0
		|| out->recoverer_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT || out->ir_request_id == 0
		|| bytes_are_zero(out->isolation_sha256, 32) || bytes_are_zero(out->closure_sha256, 32))
		goto refused;
	/* Historical validation retains the original hash and every predecessor
	 * field except its former enclosing history reference. Never follow that
	 * obsolete pointer and never interpret this as an active startup intent. */
	result = startup_decode_fields(out->original, root, node, &out->original_ref, history,
								   &out->initialization);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto refused;
	result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (out->initialization.phase != CLUSTER_WAL_STARTUP_INITIALIZING
		|| out->initialization.claim.identity.origin_owner_incarnation != ref->incarnation
		|| (out->recoverer_node == out->initialization.claim.identity.origin_node_id
			&& out->recoverer_incarnation == ref->incarnation)
		|| memcmp(out->operation_uuid, out->initialization.operation_uuid, 16) != 0
		|| out->initialization.predecessor_file_sequence >= out->sealing_sequence
		|| read_u32_le(bytes + 2068) != out->initialization.timeline)
		goto refused;
	observation = &out->observation;
	/* The selected embedded claim already binds this namespace. Keep the
	 * in-memory observation complete without adding a second disk field. */
	observation->tail.database_incarnation = out->initialization.claim.database_incarnation;
	observation->tail.complete_end = read_u64_le(bytes + 2048);
	observation->tail.last_record_start = read_u64_le(bytes + 2056);
	observation->tail.last_record_crc = read_u32_le(bytes + 2064);
	observation->tail.records = read_u64_le(bytes + 2072);
	observation->fpw_records = read_u64_le(bytes + 2080);
	observation->parameter_records = read_u64_le(bytes + 2088);
	result = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	for (size_t offset = 2096; offset <= 2112; offset += 4)
		if (read_u32_le(bytes + offset) > PG_INT32_MAX)
			goto refused;
	observation->max_connections = read_u32_le(bytes + 2096);
	observation->max_worker_processes = read_u32_le(bytes + 2100);
	observation->max_wal_senders = read_u32_le(bytes + 2104);
	observation->max_prepared_xacts = read_u32_le(bytes + 2108);
	observation->max_locks_per_xact = read_u32_le(bytes + 2112);
	if (read_u32_le(bytes + 2116) > 1)
		goto refused;
	observation->fpw_disabled = read_u32_le(bytes + 2116) != 0;
	if (observation->fpw_records > observation->tail.records
		|| observation->parameter_records != observation->tail.records - observation->fpw_records
		|| (observation->fpw_records == 0 && observation->fpw_disabled)
		|| (observation->parameter_records == 0
				? !bytes_are_zero(bytes + 2096, 20)
				: observation->max_connections == 0 || observation->max_locks_per_xact == 0))
		goto refused;
	if (observation->tail.records == 0) {
		if (observation->tail.complete_end != 0 || observation->tail.last_record_start != 0
			|| observation->tail.last_record_crc != 0)
			goto refused;
	} else {
		if (observation->tail.last_record_start
				< out->initialization.first_segment_lsn + SizeOfXLogLongPHD
			|| observation->tail.complete_end <= observation->tail.last_record_start)
			goto refused;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
refused:
	memset(out, 0, sizeof(*out));
	return result;
}

ClusterControlRootResult
cluster_wal_terminal_encode(const ControlRootImage *root, uint32 node,
							const ClusterWalHistoryImage *history,
							const ClusterWalTerminalImage *terminal,
							uint8 bytes[CLUSTER_WAL_TERMINAL_BYTES], ClusterWalTerminalRef *ref)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	ClusterWalTerminalImage check = { 0 };
	const ClusterWalStartupObservation *o;
	bool alias
		= history_ranges_overlap(root, sizeof(*root), bytes, CLUSTER_WAL_TERMINAL_BYTES)
		  || history_ranges_overlap(history, sizeof(*history), bytes, CLUSTER_WAL_TERMINAL_BYTES)
		  || history_ranges_overlap(terminal, sizeof(*terminal), bytes, CLUSTER_WAL_TERMINAL_BYTES)
		  || history_ranges_overlap(root, sizeof(*root), ref, sizeof(*ref))
		  || history_ranges_overlap(history, sizeof(*history), ref, sizeof(*ref))
		  || history_ranges_overlap(terminal, sizeof(*terminal), ref, sizeof(*ref))
		  || history_ranges_overlap(bytes, CLUSTER_WAL_TERMINAL_BYTES, ref, sizeof(*ref));
	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_WAL_TERMINAL_BYTES);
	if (ref != NULL)
		memset(ref, 0, sizeof(*ref));
	if (alias || root == NULL || history == NULL || terminal == NULL || bytes == NULL || ref == NULL
		|| node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return result;
	result = startup_decode_fields(terminal->original, root, node, &terminal->original_ref, history,
								   &check.initialization);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	o = &terminal->observation;
	if (o->tail.database_incarnation != check.initialization.claim.database_incarnation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (o->checkpoint_records != 0 || o->checkpoint_start != 0 || o->checkpoint_end != 0
		|| o->checkpoint_crc != 0 || o->checkpoint_info != 0 || o->unsupported_records != 0
		|| !bytes_are_zero((const uint8 *)&o->checkpoint, sizeof(o->checkpoint)))
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	memcpy(bytes, "PGWG", 4);
	write_u16_le(bytes + 4, 2);
	write_u16_le(bytes + 6, CLUSTER_WAL_TERMINAL_BYTES);
	write_u32_le(bytes + 8, CLUSTER_WAL_INITIALIZATION_TERMINATED);
	write_u32_le(bytes + 12, terminal->closure_version);
	memcpy(bytes + 16, terminal->operation_uuid, 16);
	write_u64_le(bytes + 32, terminal->generation);
	write_u64_le(bytes + 40, terminal->database_incarnation);
	write_u64_le(bytes + 48, terminal->sealing_sequence);
	memcpy(bytes + 56, terminal->sealing_sha256, 32);
	write_u64_le(bytes + 88, terminal->formation_epoch);
	write_u64_le(bytes + 96, terminal->recoverer_incarnation);
	write_u32_le(bytes + 104, terminal->recoverer_node);
	write_u64_le(bytes + 112, terminal->ir_request_id);
	write_u64_le(bytes + 120, terminal->original_ref.generation);
	memcpy(bytes + 128, terminal->original_ref.sha256, 32);
	memcpy(bytes + 160, terminal->isolation_sha256, 32);
	memcpy(bytes + 192, terminal->closure_sha256, 32);
	memcpy(bytes + 256, terminal->original, CLUSTER_WAL_STARTUP_BYTES);
	write_u64_le(bytes + 2048, o->tail.complete_end);
	write_u64_le(bytes + 2056, o->tail.last_record_start);
	write_u32_le(bytes + 2064, o->tail.last_record_crc);
	write_u32_le(bytes + 2068, check.initialization.timeline);
	write_u64_le(bytes + 2072, o->tail.records);
	write_u64_le(bytes + 2080, o->fpw_records);
	write_u64_le(bytes + 2088, o->parameter_records);
	write_u32_le(bytes + 2096, o->max_connections);
	write_u32_le(bytes + 2100, o->max_worker_processes);
	write_u32_le(bytes + 2104, o->max_wal_senders);
	write_u32_le(bytes + 2108, o->max_prepared_xacts);
	write_u32_le(bytes + 2112, o->max_locks_per_xact);
	write_u32_le(bytes + 2116, o->fpw_disabled ? 1 : 0);
	write_u32_le(bytes + 2300, control_root_crc(bytes, 2300));
	ref->incarnation = check.initialization.claim.identity.origin_owner_incarnation;
	ref->generation = terminal->generation;
	if (!control_root_sha256(bytes, CLUSTER_WAL_TERMINAL_BYTES, ref->sha256)) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto refused;
	}
	result = cluster_wal_terminal_decode(bytes, CLUSTER_WAL_TERMINAL_BYTES, root, node, history,
										 ref, &check);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
refused:
	memset(bytes, 0, CLUSTER_WAL_TERMINAL_BYTES);
	memset(ref, 0, sizeof(*ref));
	return result;
}

ClusterControlRootResult
cluster_control_root_v3_startup_encode(const ControlRootImage *root, uint32 origin_node,
									   const ClusterWalStartupImage *startup,
									   uint8 bytes[CLUSTER_WAL_STARTUP_BYTES],
									   ControlRootStartupRefV3 *out_ref)
{
	ClusterControlRootResult result;
	ControlRootImage *prospective;
	ClusterWalStartupImage decoded;
	ControlRootStartupRefV3 ref;
	bool alias
		= history_ranges_overlap(root, sizeof(*root), bytes, CLUSTER_WAL_STARTUP_BYTES)
		  || history_ranges_overlap(startup, sizeof(*startup), bytes, CLUSTER_WAL_STARTUP_BYTES)
		  || history_ranges_overlap(root, sizeof(*root), out_ref, sizeof(*out_ref))
		  || history_ranges_overlap(startup, sizeof(*startup), out_ref, sizeof(*out_ref))
		  || history_ranges_overlap(bytes, CLUSTER_WAL_STARTUP_BYTES, out_ref, sizeof(*out_ref));

	if (bytes != NULL)
		memset(bytes, 0, CLUSTER_WAL_STARTUP_BYTES);
	if (out_ref != NULL)
		memset(out_ref, 0, sizeof(*out_ref));
	if (alias || root == NULL || startup == NULL || bytes == NULL || out_ref == NULL
		|| origin_node >= CLUSTER_CONTROL_ROOT_RECORD_COUNT)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (root->header.format_version != CONTROL_ROOT_HEADER_VERSION_V3)
		return CLUSTER_CONTROL_ROOT_BAD_VERSION;
	if (!root->present[origin_node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	if (startup->generation == 0)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	/* Validate wide logical values before the narrow record encoding. The
	 * decoder must not accidentally bless a truncated lifecycle or identity. */
	for (unsigned i = 0; i < 2; i++) {
		const ClusterWalHistoryRecord *record
			= i == 0 ? &startup->predecessor : &startup->successor;

		if (i == 1 && startup->phase != CLUSTER_WAL_STARTUP_DURABLE) {
			if (!bytes_are_zero((const uint8 *)record, sizeof(*record)))
				return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
			continue;
		}
		result
			= snapshot_validate(&record->snapshot, origin_node + 1, root->header.system_identifier,
								root->header.storage_uuid, root->header.authority_uuid, true);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = cluster_wal_claim_v2_encode(&startup->claim, bytes + 1280);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto refused;
	memcpy(bytes, "PGWG", 4);
	write_u16_le(bytes + 4, 1);
	write_u16_le(bytes + 6, CLUSTER_WAL_STARTUP_BYTES);
	write_u32_le(bytes + 8, startup->phase);
	write_u32_le(bytes + 12, startup->input_kind);
	memcpy(bytes + 16, startup->operation_uuid, 16);
	write_u64_le(bytes + 32, startup->database_incarnation);
	write_u64_le(bytes + 40, startup->config_generation);
	write_u64_le(bytes + 48, startup->formation_epoch);
	write_u64_le(bytes + 56, startup->predecessor_file_sequence);
	memcpy(bytes + 64, startup->predecessor_file_sha256, 32);
	write_u64_le(bytes + 96, startup->generation);
	write_u64_le(bytes + 104, startup->first_segment_lsn);
	write_u32_le(bytes + 112, startup->timeline);
	write_u32_le(bytes + 116, startup->segment_size);
	memcpy(bytes + 120, startup->predecessor_evidence_sha256, 32);
	write_u64_le(bytes + 152, startup->input_record_start);
	write_u64_le(bytes + 160, startup->input_record_end);
	write_u32_le(bytes + 168, startup->input_record_crc);
	write_u32_le(bytes + 172, startup->input_timeline);
	write_u64_le(bytes + 176, startup->sealed_input_end);
	encode_record_version(
		bytes + 256, &startup->predecessor.snapshot, startup->predecessor.publisher_incarnation,
		startup->predecessor.publisher_node,
		(ClusterControlRootPublishReason)startup->predecessor.snapshot.lifecycle_reason,
		CONTROL_ROOT_RECORD_VERSION_V2, &startup->predecessor.refs, NULL);
	if (startup->phase == CLUSTER_WAL_STARTUP_DURABLE) {
		encode_record_version(
			bytes + 768, &startup->successor.snapshot, startup->successor.publisher_incarnation,
			startup->successor.publisher_node,
			(ClusterControlRootPublishReason)startup->successor.snapshot.lifecycle_reason,
			CONTROL_ROOT_RECORD_VERSION_V2, &startup->successor.refs, NULL);
	}
	write_u32_le(bytes + 1532, control_root_crc(bytes, 1532));
	memset(&ref, 0, sizeof(ref));
	ref.generation = startup->generation;
	if (!control_root_sha256(bytes, CLUSTER_WAL_STARTUP_BYTES, ref.sha256)) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto refused;
	}
	/* Qualify the exact future binding without changing the selected root.
	 * A successful encoding is not a successful CAS or an admission decision. */
	prospective = palloc(sizeof(*prospective));
	*prospective = *root;
	prospective->startup[origin_node] = ref;
	result = cluster_control_root_v3_startup_decode(bytes, CLUSTER_WAL_STARTUP_BYTES, prospective,
													origin_node, &decoded);
	pfree(prospective);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out_ref = ref;
		return result;
	}
refused:
	memset(bytes, 0, CLUSTER_WAL_STARTUP_BYTES);
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
static ClusterControlRootResult
read_control_version(const uint8 storage_uuid[16], uint64 system_identifier, ControlRootImage *root,
					 ControlFileData *common, ClusterControlRootFileToken *token, uint16 version)
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
	root_result = read_canonical_pair_version(root, bak, storage_uuid, system_identifier, version);
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
static ClusterControlRootResult
read_thread_input(const ClusterControlRootIdentity *self, ControlRootImage *root,
				  ControlFileData *out, ClusterControlRootFileToken *token, uint16 version,
				  bool native_input)
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
	root_result = read_control_version(self->storage_uuid, self->system_identifier, root, &common,
									   token, version);
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
	result = native_input ? cluster_recovery_anchor_v2_read_native_locked(&ref, &common, out)
						  : cluster_recovery_anchor_v2_read_locked(&ref, &common, out);
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

static ClusterControlRootResult
read_thread_version(const ClusterControlRootIdentity *self, ControlRootImage *root,
					ControlFileData *out, ClusterControlRootFileToken *token, uint16 version)
{
	return read_thread_input(self, root, out, token, version, false);
}

ClusterControlRootResult
cluster_control_root_v2_read_control_locked(const uint8 storage_uuid[16], uint64 system_identifier,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token)
{
	return read_control_version(storage_uuid, system_identifier, root, common, token,
								CONTROL_ROOT_HEADER_VERSION_V2);
}

ClusterControlRootResult
cluster_control_root_v2_read_thread_locked(const ClusterControlRootIdentity *self,
										   ControlRootImage *root, ControlFileData *out,
										   ClusterControlRootFileToken *token)
{
	return read_thread_version(self, root, out, token, CONTROL_ROOT_HEADER_VERSION_V2);
}

ClusterControlRootResult
cluster_control_root_v3_read_control_locked(const uint8 storage_uuid[16], uint64 system_identifier,
											ControlRootImage *root, ControlFileData *common,
											ClusterControlRootFileToken *token)
{
	return read_control_version(storage_uuid, system_identifier, root, common, token,
								CONTROL_ROOT_HEADER_VERSION_V3);
}

ClusterControlRootResult
cluster_control_root_v3_read_thread_locked(const ClusterControlRootIdentity *self,
										   ControlRootImage *root, ControlFileData *out,
										   ClusterControlRootFileToken *token)
{
	return read_thread_version(self, root, out, token, CONTROL_ROOT_HEADER_VERSION_V3);
}

/* PGRAC: derive observations for every configured predecessor, never an
 * ALIVE-based subset. A decoded clean state is still not exit evidence.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_control_root_v3_clean_exit_cut(const uint8 *bytes, Size length,
									   const uint8 storage_uuid[16], uint64 system_identifier,
									   const ClusterFormationSnapshotV1 *formation,
									   ClusterStartupExitCut *out)
{
	ControlRootImage *root;
	ClusterStartupExitCut cut = { 0 };
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(bytes, length, out, sizeof(*out))
				 || history_ranges_overlap(storage_uuid, 16, out, sizeof(*out))
				 || history_ranges_overlap(formation, sizeof(*formation), out, sizeof(*out));
	int coordinator = -1;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || out == NULL || formation == NULL || bytes == NULL || storage_uuid == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (formation->local_epoch == 0 || formation->prebump_sync_active != 0
		|| formation->self_join_failed != 0
		|| !bytes_are_zero(formation->reserved, sizeof(formation->reserved))
		|| !bytes_are_zero(formation->pending_join_bitmap, sizeof(formation->pending_join_bitmap)))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	root = palloc(sizeof(*root));
	result = cluster_control_root_v3_decode(bytes, length, storage_uuid, system_identifier, root);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_CLOSED) {
		result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
		goto done;
	}
	memcpy(cut.required, root->header.v2.configured, sizeof(cut.required));
	result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		uint8 bit = (uint8)(1u << (node % 8));
		if ((cut.required[node / 64] & (UINT64_C(1) << (node % 64))) == 0) {
			if (formation->membership.membership_state[node] == CLUSTER_MEMBER_MEMBER)
				goto done;
			continue;
		}
		if (!root->present[node] || root->startup[node].generation != 0
			|| root->records[node].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
			|| formation->membership.membership_state[node] != CLUSTER_MEMBER_MEMBER
			|| ((formation->excluded_bitmap[node / 8] | formation->clean_departed_bitmap[node / 8]
				 | formation->removed_bitmap[node / 8])
				& bit)
				   != 0)
			goto done;
		cut.predecessor[node] = root->records[node].identity.origin_owner_incarnation;
		cut.observer[node] = formation->membership.last_admitted_incarnation[node];
		if (cut.predecessor[node] == 0 || cut.observer[node] <= cut.predecessor[node])
			goto done;
		/* Existing formation coordinator rule: the lowest MEMBER, not a
		 * new election or an independently liveness-filtered candidate. */
		if (coordinator < 0)
			coordinator = (int)node;
	}
	if (coordinator < 0)
		goto done;
	cut.key.coordinator = (uint32)coordinator;
	cut.key.coordinator_incarnation = cut.observer[coordinator];
	cut.key.epoch = formation->local_epoch;
	cut.key.root_sequence = root->header.file_txn_seq;
	cut.key.system_identifier = root->header.system_identifier;
	cut.key.database_incarnation = root->header.v2.database_incarnation;
	cut.key.config_generation = root->header.v2.config_generation;
	memcpy(cut.key.storage_uuid, root->header.storage_uuid, 16);
	memcpy(cut.key.authority_uuid, root->header.authority_uuid, 16);
	if (!control_root_sha256(root->bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES, cut.key.root_sha256)) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	*out = cut;
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	pfree(root);
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

static ClusterControlRootResult
read_runtime_local_version(ControlFileData *out, uint16 version)
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
	result = read_control_version(storage_uuid, sysid, root, &common, &token, version);
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
	result = read_thread_version(&self, root, &thread, &token, version);
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

ClusterControlRootResult
cluster_control_root_v2_read_runtime_local_locked(ControlFileData *out)
{
	return read_runtime_local_version(out, CONTROL_ROOT_HEADER_VERSION_V2);
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
static ClusterControlRootResult
read_retention_version(const ClusterControlRootIdentity *self, ClusterControlRootSnapshot *out,
					   ClusterControlRootReadToken *token, uint16 version)
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
				result = read_thread_version(&expected, root, &thread, &file_token, version);
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

ClusterControlRootResult
cluster_control_root_v2_read_retention_current(const ClusterControlRootIdentity *self,
											   ClusterControlRootSnapshot *out,
											   ClusterControlRootReadToken *token)
{
	return read_retention_version(self, out, token, CONTROL_ROOT_HEADER_VERSION_V2);
}

/* PGRAC: one process-local observation, not a cached CF authorization. A
 * service caller may receive it only after the original exact release and
 * only at the same input/cut. A background poll can retire the lock even if
 * this caller never asks again. Author: SqlRush <sqlrush@gmail.com> */
typedef struct CanonicalAuxRead {
	bool pending;
	bool has_expected;
	bool stop_observation;
	bool recovery_observation;
	uint16 format_version;
	uint16 thread;
	uint64 admitted_incarnation;
	ClusterControlRootStopObservation stop;
	uint64 cookie, epoch, generation, routing;
	int32 master;
	ClusterControlRootIdentity expected;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRecoverySubject recovery;
	ClusterControlRootResult result;
} CanonicalAuxRead;

static CanonicalAuxRead canonical_aux_read;

static bool
canonical_aux_actor(void)
{
	return cluster_shared_config && (MyBackendType == B_LMON || MyBackendType == B_LMS);
}

static bool
canonical_aux_capture_cut(CanonicalAuxRead *read)
{
	const ClusterResId resid
		= { .type = CLUSTER_CF_RESID_TYPE, .lockmethodid = DEFAULT_LOCKMETHOD };

	read->epoch = cluster_epoch_get_current();
	read->generation = cluster_grd_redeclare_generation();
	read->master = cluster_grd_lookup_master_gen(&resid, &read->routing);
	return read->epoch != 0 && read->master >= 0 && read->routing != 0
		   && read->epoch == cluster_epoch_get_current();
}

static bool
canonical_aux_finish(uint16 thread, const ClusterControlRootIdentity *expected,
					 ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token,
					 uint64 admitted_incarnation, ClusterControlRootStopObservation *stop,
					 ClusterControlRootResult *result, uint16 version,
					 ClusterControlRecoverySubject *recovery)
{
	CanonicalAuxRead now = { 0 };
	CanonicalAuxRead *pending = &canonical_aux_read;
	bool valid;

	cluster_cf_retirement_poll();
	if (!pending->pending)
		return false;
	*result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!cluster_cf_release_completed(ShareLock, pending->cookie)) {
		/* A different completed owner is not this owner's release proof.
		 * Discard the observation when no matching pending hold remains. */
		if (!cluster_cf_held(ShareLock))
			memset(pending, 0, sizeof(*pending));
		return true;
	}
	valid
		= pending->format_version == version && pending->thread == thread
		  && pending->has_expected == (expected != NULL)
		  && pending->stop_observation == (stop != NULL)
		  && pending->recovery_observation == (recovery != NULL)
		  && pending->admitted_incarnation == admitted_incarnation
		  && (expected == NULL || cluster_control_root_identity_equal(&pending->expected, expected))
		  && canonical_aux_capture_cut(&now) && now.epoch == pending->epoch
		  && now.generation == pending->generation && now.master == pending->master
		  && now.routing == pending->routing;
	if (valid) {
		*result = pending->result;
		if (*result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| *result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
			if (out != NULL)
				*out = pending->snapshot;
			if (token != NULL)
				*token = pending->token;
			if (stop != NULL) {
				*stop = pending->stop;
			}
			if (recovery != NULL)
				*recovery = pending->recovery;
		}
	}
	memset(pending, 0, sizeof(*pending));
	return true;
}

/* PGRAC: the exact persistent tail is required for current and historical
 * clean records alike; a selected shutdown anchor alone is insufficient.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
shutdown_v2_record_input(const ControlRootImage *root, const ClusterControlRootSnapshot *record,
						  const ControlRootRecordRefsV2 *refs, const ControlFileData *raw)
{
	ClusterWalSourceRef ref = { 0 };
	ClusterWalTailObservation tail;
	ClusterControlRootResult result;

	if (raw->state != DB_SHUTDOWNED || raw->checkPointCopy.redo != raw->checkPoint
		|| record->checkpoint_lower_lsn == InvalidXLogRecPtr
		|| record->checkpoint_lower_lsn > raw->checkPoint
		|| (root->header.format_version < 3 && record->checkpoint_lower_lsn != raw->checkPoint)
		|| (record->root_flags
			& (CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
			   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID))
			   != (CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
				   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID)
		|| record->tail_tli != record->checkpoint_tli
		|| record->tail_last_record_lsn != raw->checkPoint
		|| record->tail_last_record_crc32c != record->checkpoint_record_crc32c
		|| record->validated_tail_lsn_exclusive <= raw->checkPoint)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	ref.claim.identity = record->identity;
	ref.claim.database_incarnation = root->header.v2.database_incarnation;
	ref.claim.max_config_generation = root->header.v2.config_generation;
	memcpy(ref.claim.claim_sha256, refs->claim_sha256, 32);
	ref.timeline = record->checkpoint_tli;
	result = cluster_wal_tail_observe_checkpoint(
		cluster_wal_threads_dir, &ref, wal_segment_size, record->checkpoint_lower_lsn,
		record->validated_tail_lsn_exclusive, raw->checkPoint, record->checkpoint_record_crc32c,
		&tail);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (tail.complete_end != record->validated_tail_lsn_exclusive
		|| tail.last_record_start != record->tail_last_record_lsn
		|| tail.last_record_crc != record->tail_last_record_crc32c)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* The selected record may be retained history or a configured non-serving
 * peer. Do not impose the current serving roster on its old writer identity,
 * and do not infer recovery-terminal proof from a lifecycle enum. */
static ClusterControlRootResult
closed_v2_record_locked(const ControlRootImage *root, const ClusterControlRootSnapshot *record,
						const ControlRootRecordRefsV2 *refs, const ControlFileData *common)
{
	ClusterRecoveryAnchorRefV2 anchor = { 0 };
	ClusterWalThreadClaimRefV2 claim_ref = { 0 };
	ClusterWalThreadClaimV2 claim;
	ControlFileData raw;
	ClusterControlRootResult result;

	if (record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	claim_ref.identity = record->identity;
	claim_ref.database_incarnation = root->header.v2.database_incarnation;
	claim_ref.max_config_generation = root->header.v2.config_generation;
	memcpy(claim_ref.claim_sha256, refs->claim_sha256, 32);
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &claim_ref, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	anchor.identity = record->identity;
	anchor.database_incarnation = root->header.v2.database_incarnation;
	anchor.max_config_generation = root->header.v2.config_generation;
	anchor.anchor_generation = refs->anchor_generation;
	memcpy(anchor.anchor_sha256, refs->anchor_sha256, 32);
	memcpy(anchor.claim_sha256, refs->claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_locked(&anchor, common, &raw);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_recovery_anchor_v2_thread_state(&anchor, record, &raw);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return shutdown_v2_record_input(root, record, refs, &raw);
}

/* The projected native view deliberately says IN_PRODUCTION while the
 * writer is OPEN. A normal-stop consumer must inspect the selected raw
 * anchor instead; neither that projection nor a legacy STOPPED slot proves
 * a shutdown checkpoint. Caller owns CF-S throughout these exact reads. */
static ClusterControlRootResult
stop_phase_v2_locked(const ControlRootImage *root, uint32 index, const ControlFileData *view,
					 uint64 admitted_incarnation, ClusterControlRootStopPhase *phase)
{
	const ClusterControlRootSnapshot *record = &root->records[index];
	ClusterRecoveryAnchorRefV2 anchor = { 0 };
	ControlFileData raw;
	ClusterControlRootResult result;

	*phase = CLUSTER_CONTROL_ROOT_STOP_UNKNOWN;
	if (record->identity.origin_node_id != (int32)index
		|| record->identity.origin_owner_incarnation != admitted_incarnation
		|| record->identity.thread_claim_created_at <= 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| (root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
			&& root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_CLOSED)
		|| (record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			&& record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
		|| (root->header.v2.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED
			&& record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED)
		|| (root->header.v2.serving[index / 64] & (UINT64_C(1) << (index % 64))) == 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	anchor.identity = record->identity;
	anchor.database_incarnation = root->header.v2.database_incarnation;
	anchor.max_config_generation = root->header.v2.config_generation;
	anchor.anchor_generation = root->refs[index].anchor_generation;
	memcpy(anchor.anchor_sha256, root->refs[index].anchor_sha256, 32);
	memcpy(anchor.claim_sha256, root->refs[index].claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_locked(&anchor, view, &raw);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (raw.state == DB_IN_PRODUCTION) {
		if (record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
			return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
		*phase = CLUSTER_CONTROL_ROOT_STOP_ACTIVE;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	result = shutdown_v2_record_input(root, record, &root->refs[index], &raw);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	*phase = record->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
				 ? CLUSTER_CONTROL_ROOT_STOP_CLOSED
				 : CLUSTER_CONTROL_ROOT_STOP_CHECKPOINT;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: discover the exact failed generation without relabeling its clean
 * predecessor. Only a value observation leaves this CF interval; physical
 * scanning and recovery still require independent isolation/WALR/IR owners.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult
read_recovery_subject_locked(uint16 thread_id, const uint8 storage_uuid[16],
							 uint64 system_identifier, const ClusterControlRootIdentity *expected,
							 ControlRootImage *root, ClusterControlRecoverySubject *out)
{
	ClusterControlRootFileToken before, token;
	ClusterControlRootIdentity current;
	ControlFileData view;
	ClusterWalOriginInputs *inputs;
	ClusterControlRootResult result;
	uint32 node = thread_id - 1;
	memset(out, 0, sizeof(*out));
	result = read_control_version(storage_uuid, system_identifier, root, &view, &before, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!root->present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	current = root->records[node].identity;
	result = read_thread_version(&current, root, &view, &token, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&before, &token, sizeof(token)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	inputs = palloc0(sizeof(*inputs));
	result = cluster_wal_origin_inputs_read_locked(root, node, inputs);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && inputs->has_pending) {
		const ClusterWalStartupImage *op = &inputs->pending;
		ClusterWalSourceRef ref = { 0 };
		ClusterWalThreadClaimV2 claim;
		uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
		if ((op->phase != CLUSTER_WAL_STARTUP_INITIALIZING
			 && op->phase != CLUSTER_WAL_STARTUP_DURABLE)
			|| (root->header.v2.serving[node / 64] & (UINT64_C(1) << (node % 64))) != 0) {
			result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			goto done;
		}
		ref.claim.identity = op->claim.identity;
		ref.claim.database_incarnation = op->database_incarnation;
		ref.claim.max_config_generation = op->config_generation;
		ref.timeline = op->timeline;
		result = cluster_wal_claim_v2_encode(&op->claim, bytes);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		if (!control_root_sha256(bytes, sizeof(bytes), ref.claim.claim_sha256)) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
		result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &ref.claim, &claim);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		/* Discover the exact obligation, not whether its WAL is empty or
		 * complete. The isolated recovery owner performs that census; an
		 * obsolete group-flush prefix is not an identity prerequisite. */
		out->kind = CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER;
		out->duty = op->claim.identity;
		out->pending.file = token;
		out->pending.generation = root->startup[node].generation;
		memcpy(out->pending.sha256, root->startup[node].sha256, 32);
		memcpy(out->pending.operation_uuid, op->operation_uuid, 16);
	} else if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		out->kind = CLUSTER_CONTROL_RECOVERY_CURRENT_CHECKPOINT;
		out->duty = current;
		out->current = root->records[node];
		make_read_token(root, thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &out->current_token);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && expected != NULL
		&& !cluster_control_root_identity_equal(expected, &out->duty))
		result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
done:
	pfree(inputs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

/* PGRAC: recovery owners may inspect a failed peer, not only their own live
 * writer. Select and authenticate the v2 root/config/claim/anchor under one
 * owned CF-S interval. This mints an observation, never serving permission,
 * isolation proof, a retention pin or permission to finish recovery.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static ClusterControlRootResult
read_canonical_version(uint16 thread_id, const ClusterControlRootIdentity *expected_identity,
					   ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token,
					   uint64 admitted_incarnation, ClusterControlRootStopObservation *stop,
					   uint16 version, ClusterControlRecoverySubject *recovery)
{
	ControlRootImage *root;
	ControlRootImage *peer_root;
	ControlFileData thread;
	ClusterControlRootIdentity expected;
	ClusterControlRootFileToken discovered, file_token;
	ClusterControlRootReadToken selected;
	ClusterControlRootResult result;
	volatile bool held = false;
	uint8 storage_uuid[16];
	uint64 system_identifier;
	uint32 index;
	bool auxiliary = canonical_aux_actor();
	CanonicalAuxRead continuation = { 0 };
	ClusterControlRootStopObservation stop_sample = { 0 };
	ClusterControlRecoverySubject subject = { 0 };

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (token != NULL)
		memset(token, 0, sizeof(*token));
	if (stop != NULL)
		memset(stop, 0, sizeof(*stop));
	if (recovery != NULL)
		memset(recovery, 0, sizeof(*recovery));
	if (!cluster_shared_config || !cluster_enabled || !cluster_controlfile_shared_authority
		|| thread_id == 0 || thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| (stop != NULL && (admitted_incarnation == 0 || admitted_incarnation == UINT64_MAX)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
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
	if (auxiliary
		&& canonical_aux_finish(thread_id, expected_identity, out, token, admitted_incarnation,
								stop, &result, version, recovery))
		return result;
	if (cluster_cf_held(ExclusiveLock)
		|| (cluster_cf_held(ShareLock) && !(auxiliary && cluster_cf_acquire_pending(ShareLock))))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	index = thread_id - 1;
	root = palloc(sizeof(*root));
	peer_root = stop != NULL ? palloc(sizeof(*peer_root)) : NULL;
	result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	PG_TRY();
	{
		if (auxiliary ? cluster_cf_lock_poll(ShareLock) : cluster_cf_lock(ShareLock)) {
			held = true;
			if (cluster_cf_held_is_clusterwide(ShareLock)
				&& (!auxiliary || canonical_aux_capture_cut(&continuation))) {
				if (recovery != NULL)
					result
						= read_recovery_subject_locked(thread_id, storage_uuid, system_identifier,
													   expected_identity, root, &subject);
				else {
					if (expected_identity == NULL) {
						result = read_control_version(storage_uuid, system_identifier, root,
													  &thread, &discovered, version);
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
						result
							= read_thread_version(&expected, root, &thread, &file_token, version);
						/* A current-record-only observation cannot certify an unfinished
					 * successor as the clean predecessor. Full-stop observation must
					 * also retain non-serving pending participants. */
						if ((result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
							 || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
							&& version == CONTROL_ROOT_HEADER_VERSION_V3) {
							for (uint32 peer = 0; peer < CLUSTER_CONTROL_ROOT_RECORD_COUNT; peer++)
								if ((peer == index || stop != NULL)
									&& root->startup[peer].generation != 0) {
									result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
									break;
								}
						}
						if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
							|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
							if (expected_identity == NULL
								&& memcmp(&discovered, &file_token, sizeof(file_token)) != 0)
								result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
							else {
								if (stop != NULL) {
									/* Keep the original selected root immutable. Each
								 * serving thread must authenticate its own claim,
								 * anchor and phase under this same CF interval. */
									for (uint32 peer = 0; peer < CLUSTER_CONTROL_ROOT_RECORD_COUNT;
										 peer++) {
										ControlFileData peer_view;
										ClusterControlRootFileToken peer_token;
										const ClusterControlRootIdentity *id
											= &root->records[peer].identity;
										if ((root->header.v2.serving[peer / 64]
											 & (UINT64_C(1) << (peer % 64)))
											== 0)
											continue;
										result = read_thread_version(id, peer_root, &peer_view,
																	 &peer_token, version);
										if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
											&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
											break;
										if (memcmp(&peer_token, &file_token, sizeof(peer_token))
											!= 0) {
											result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
											break;
										}
										result = stop_phase_v2_locked(
											peer_root, peer, &peer_view,
											id->origin_owner_incarnation,
											&stop_sample.members[peer].phase);
										if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
											break;
										stop_sample.members[peer].incarnation
											= id->origin_owner_incarnation;
										stop_sample.members[peer].claim_created_at
											= id->thread_claim_created_at;
									}
									if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
										result = stop_phase_v2_locked(root, index, &thread,
																	  admitted_incarnation,
																	  &stop_sample.phase);
								}
								make_read_token(root, thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
												&selected);
							}
						}
					}
				}
			}
			if (auxiliary) {
				continuation.cookie = cluster_cf_owner_cookie(ShareLock);
				continuation.thread = thread_id;
				continuation.format_version = version;
				continuation.has_expected = expected_identity != NULL;
				continuation.stop_observation = stop != NULL;
				continuation.recovery_observation = recovery != NULL;
				continuation.admitted_incarnation = admitted_incarnation;
				if (expected_identity != NULL)
					continuation.expected = *expected_identity;
				continuation.result = result;
				if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
					|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
					if (recovery != NULL)
						continuation.recovery = subject;
					else {
						continuation.snapshot = root->records[index];
						continuation.token = selected;
					}
					if (stop != NULL) {
						stop_sample.snapshot = root->records[index];
						stop_sample.token = selected;
						continuation.stop = stop_sample;
					}
				}
			}
			result = release_cf(ShareLock, result);
			held = false;
			if (auxiliary) {
				/* Immediate release is not permission to publish an old-cut
				 * observation either. Use the same exact completion/cut check
				 * as a release completed by a later service iteration. */
				result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
				if (continuation.cookie != 0) {
					continuation.pending = true;
					canonical_aux_read = continuation;
					(void)canonical_aux_finish(thread_id, expected_identity, out, token,
											   admitted_incarnation, stop, &result, version,
											   recovery);
				}
			}
		}
	}
	PG_CATCH();
	{
		ClusterControlRootResult cleanup = CLUSTER_CONTROL_ROOT_IO_ERROR;

		if (held)
			cleanup = release_cf(ShareLock, cleanup);
		if (peer_root != NULL)
			pfree(peer_root);
		pfree(root);
		if (!auxiliary && cleanup == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm canonical control-root read-lock cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		if (recovery != NULL)
			*recovery = subject;
		if (out != NULL)
			*out = root->records[index];
		if (token != NULL)
			*token = selected;
		if (stop != NULL) {
			stop_sample.snapshot = root->records[index];
			stop_sample.token = selected;
			*stop = stop_sample;
		}
	}
	if (peer_root != NULL)
		pfree(peer_root);
	pfree(root);
	if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
		elog(FATAL, "could not confirm canonical control-root read-lock release");
	return result;
}

static ClusterControlRootResult
read_canonical_v2(uint16 thread, const ClusterControlRootIdentity *expected,
				  ClusterControlRootSnapshot *out, ClusterControlRootReadToken *token,
				  uint64 admitted_incarnation, ClusterControlRootStopObservation *stop)
{
	return read_canonical_version(thread, expected, out, token, admitted_incarnation, stop,
								  CONTROL_ROOT_HEADER_VERSION_V2, NULL);
}

ClusterControlRootResult
cluster_control_root_v2_read_canonical(uint16 thread, const ClusterControlRootIdentity *expected,
									   ClusterControlRootSnapshot *out,
									   ClusterControlRootReadToken *token)
{
	return read_canonical_v2(thread, expected, out, token, 0, NULL);
}

ClusterControlRootResult
cluster_control_root_v2_stop_phase_read(uint16 thread, uint64 admitted_incarnation,
										ClusterControlRootStopObservation *out)
{
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return read_canonical_v2(thread, NULL, NULL, NULL, admitted_incarnation, out);
}

ClusterControlRootResult
cluster_control_root_v3_read_runtime_local_locked(ControlFileData *out)
{
	return read_runtime_local_version(out, CONTROL_ROOT_HEADER_VERSION_V3);
}

ClusterControlRootResult
cluster_control_root_v3_read_retention_current(const ClusterControlRootIdentity *self,
											   ClusterControlRootSnapshot *out,
											   ClusterControlRootReadToken *token)
{
	return read_retention_version(self, out, token, CONTROL_ROOT_HEADER_VERSION_V3);
}

ClusterControlRootResult
cluster_control_root_v3_read_canonical(uint16 thread, const ClusterControlRootIdentity *expected,
									   ClusterControlRootSnapshot *out,
									   ClusterControlRootReadToken *token)
{
	return read_canonical_version(thread, expected, out, token, 0, NULL,
								  CONTROL_ROOT_HEADER_VERSION_V3, NULL);
}

ClusterControlRootResult
cluster_control_root_v3_stop_phase_read(uint16 thread, uint64 admitted_incarnation,
										ClusterControlRootStopObservation *out)
{
	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return read_canonical_version(thread, NULL, NULL, NULL, admitted_incarnation, out,
								  CONTROL_ROOT_HEADER_VERSION_V3, NULL);
}

ClusterControlRootResult
cluster_control_root_read_recovery_subject(uint16 origin_thread,
										   const ClusterControlRootIdentity *expected,
										   ClusterControlRecoverySubject *out)
{
	if (out == NULL || history_ranges_overlap(expected, sizeof(*expected), out, sizeof(*out)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	return read_canonical_version(origin_thread, expected, NULL, NULL, 0, NULL,
								  CONTROL_ROOT_HEADER_VERSION_V3, out);
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
		return cluster_control_root_v3_read_canonical(origin_thread_id, expected_identity,
													  out_snapshot, out_token);
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
		return cluster_control_root_v3_read_canonical(origin_thread_id, NULL, out_snapshot,
													  out_token);
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
		result = cluster_control_root_v3_read_canonical(thread_id, NULL, &snapshot, &token);
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

/* PRE2: the external-fence rejoin consumer must authenticate a typed
 * checkpoint-less terminal from the same root cut that supplied the failed
 * identity.  A lifecycle value alone is never sufficient: the terminal is
 * selected through the root's retained PGWG union, its embedded initializer
 * claim must match the failed identity, and a replacement initializer for the
 * same origin must not be pending.  This function owns only a CF-S read
 * interval; it grants no recovery, serving, or provider authority. */
ClusterControlRootResult
cluster_control_root_v3_validate_rejoin_terminal(
	const ClusterControlRootIdentity *expected_identity, uint64 failed_incarnation,
	const ClusterControlRootSnapshot *expected_snapshot,
	const ClusterControlRootReadToken *expected_token,
	ClusterControlRootRejoinTerminalProofV1 *out_proof)
{
	ClusterControlRootSnapshot observed_snapshot;
	ClusterControlRootReadToken observed_token;
	ControlRootImage *root = NULL;
	ControlFileData common;
	ClusterControlRootFileToken file_token;
	ClusterControlRootReadToken current_token;
	ClusterWalHistoryImage history;
	ClusterWalTerminalImage terminal;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	uint32 node;
	volatile bool held = false;

	if (out_proof != NULL)
		memset(out_proof, 0, sizeof(*out_proof));
	if (expected_identity == NULL || expected_snapshot == NULL || expected_token == NULL
		|| out_proof == NULL || failed_incarnation == 0 || expected_identity->origin_thread_id == 0
		|| expected_identity->origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| expected_identity->origin_node_id < 0
		|| expected_identity->origin_node_id >= CLUSTER_MAX_NODES
		|| expected_identity->root_lineage_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	/* First obtain a strong cut through the normal public reader.  The caller's
	 * snapshot/token must still denote that exact cut; otherwise the rejoin
	 * operation is stale and must be retried from the root lookup. */
	memset(&observed_snapshot, 0, sizeof(observed_snapshot));
	memset(&observed_token, 0, sizeof(observed_token));
	result = cluster_control_root_v3_read_canonical(expected_identity->origin_thread_id,
													expected_identity, &observed_snapshot,
													&observed_token);
	if ((result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		 && result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		|| memcmp(&observed_snapshot, expected_snapshot, sizeof(observed_snapshot)) != 0
		|| !read_token_equal(&observed_token, expected_token))
		return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED
					   || result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				   ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
				   : result;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	root = palloc0(sizeof(*root));
	PG_TRY();
	{
		if (!cluster_cf_lock(ShareLock)) {
			result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			goto terminal_done;
		}
		held = true;
		result = read_control_version(expected_identity->storage_uuid,
									  expected_identity->system_identifier, root, &common,
									  &file_token, CONTROL_ROOT_HEADER_VERSION_V3);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			make_read_token(root, expected_identity->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
							&current_token);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			goto terminal_done;
		if (!read_token_equal(&observed_token, &current_token)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto terminal_done;
		}
		node = expected_identity->origin_thread_id - 1;
		if (!root->present[node]
			|| !cluster_control_root_identity_equal(expected_identity,
													&root->records[node].identity)
			|| root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
			|| (root->header.v2.serving[node / 64] & (UINT64_C(1) << (node % 64))) != 0
			|| root->startup[node].generation != 0
			|| (root->records[node].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
				&& root->records[node].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED
				&& root->records[node].lifecycle
					   != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)) {
			result = root->startup[node].generation != 0 ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
														 : CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			goto terminal_done;
		}
		result = cluster_wal_history_read_locked(root, node, &history);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto terminal_done;
		result = CLUSTER_CONTROL_ROOT_ABSENT;
		for (uint32 i = 0; i < history.terminal_count; i++) {
			const ClusterWalTerminalRef *ref = &history.terminals[i];

			if (ref->incarnation != failed_incarnation)
				continue;
			result = cluster_wal_terminal_read_locked(root, node, i, &terminal);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				break;
			if (!rejoin_terminal_namespace_equal(expected_identity,
												 &terminal.initialization.claim.identity)
				|| terminal.initialization.claim.identity.origin_owner_incarnation
					   != failed_incarnation
				|| terminal.initialization.claim.identity.root_lineage_seq
					   != expected_identity->root_lineage_seq + 1
				|| terminal.initialization.claim.identity.thread_claim_created_at == 0
				|| terminal.initialization.claim.identity.thread_claim_crc32c == 0
				|| terminal.generation != ref->generation
				|| bytes_are_zero(terminal.operation_uuid, sizeof(terminal.operation_uuid))
				|| bytes_are_zero(terminal.isolation_sha256, sizeof(terminal.isolation_sha256))
				|| bytes_are_zero(terminal.closure_sha256, sizeof(terminal.closure_sha256))) {
				result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
				break;
			}
			out_proof->failed_incarnation = ref->incarnation;
			out_proof->terminal_generation = ref->generation;
			memcpy(out_proof->terminal_sha256, ref->sha256, sizeof(out_proof->terminal_sha256));
			memcpy(out_proof->operation_uuid, terminal.operation_uuid,
				   sizeof(out_proof->operation_uuid));
			result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
			break;
		}
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			memset(out_proof, 0, sizeof(*out_proof));
	terminal_done:
		if (held) {
			ClusterControlRootResult release_result = release_cf(ShareLock, result);

			held = false;
			result = release_result;
		}
	}
	PG_CATCH();
	{
		ClusterControlRootResult cleanup = CLUSTER_CONTROL_ROOT_IO_ERROR;

		if (held)
			cleanup = release_cf(ShareLock, cleanup);
		if (root != NULL)
			pfree(root);
		if (cleanup == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm rejoin terminal read-lock cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (root != NULL)
		pfree(root);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out_proof, 0, sizeof(*out_proof));
	return result;
}

ClusterControlRootResult
cluster_control_root_v3_terminal_history_blocked(
	const ClusterControlRootIdentity *expected_identity, bool *out_blocked)
{
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ControlRootImage *root = NULL;
	ControlFileData common;
	ClusterControlRootFileToken file_token;
	ClusterControlRootReadToken current_token;
	ClusterWalHistoryImage history;
	ClusterControlRootResult result;
	volatile bool held = false;

	if (out_blocked != NULL)
		*out_blocked = true;
	if (expected_identity == NULL || out_blocked == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(&snapshot, 0, sizeof(snapshot));
	memset(&token, 0, sizeof(token));
	result = cluster_control_root_v3_read_canonical(expected_identity->origin_thread_id,
													expected_identity, &snapshot, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	root = palloc0(sizeof(*root));
	PG_TRY();
	{
		if (!cluster_cf_lock(ShareLock)) {
			result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
			goto retention_done;
		}
		held = true;
		result = read_control_version(expected_identity->storage_uuid,
									  expected_identity->system_identifier, root, &common,
									  &file_token, CONTROL_ROOT_HEADER_VERSION_V3);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			make_read_token(root, expected_identity->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
							&current_token);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
			goto retention_done;
		if (!read_token_equal(&token, &current_token)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto retention_done;
		}
		result = cluster_wal_history_read_locked(root, expected_identity->origin_thread_id - 1,
												 &history);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			*out_blocked = history.terminal_count != 0;
	retention_done:
		if (held) {
			ClusterControlRootResult release_result = release_cf(ShareLock, result);

			held = false;
			result = release_result;
		}
	}
	PG_CATCH();
	{
		ClusterControlRootResult cleanup = CLUSTER_CONTROL_ROOT_IO_ERROR;

		if (held)
			cleanup = release_cf(ShareLock, cleanup);
		if (root != NULL)
			pfree(root);
		if (cleanup == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm terminal retention read-lock cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (root != NULL)
		pfree(root);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out_blocked = true;
	return result;
}

/* PGRAC: provider rejoin consumes membership authority, not retained restart
 * inputs. Authenticate the exact selected terminal but defer its retirement
 * to the serving successor's checkpoint, which transfers all obligations.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_control_root_v3_consume_rejoin_terminal(
	const ClusterControlRootIdentity *expected_identity, uint64 candidate_incarnation,
	const ClusterControlRootSnapshot *expected_snapshot,
	const ClusterControlRootReadToken *expected_token,
	const ClusterControlRootRejoinTerminalProofV1 *proof, bool *out_consumed)
{
	ClusterControlRootRejoinTerminalProofV1 selected;
	ClusterControlRootResult result;

	if (out_consumed != NULL)
		*out_consumed = false;
	if (proof == NULL || out_consumed == NULL || proof->failed_incarnation == 0
		|| candidate_incarnation <= proof->failed_incarnation)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_v3_validate_rejoin_terminal(
		expected_identity, proof->failed_incarnation, expected_snapshot, expected_token, &selected);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&selected, proof, sizeof(selected)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
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

/* PGRAC: configuration has one root publication, not a local auto.conf truth.
 * Keep all fallible ownership on heap so ERROR unwinds staging and CF without
 * reverting a possibly published root. No native assign hooks run under CF.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct ConfigPublishWork {
	ControlRootImage base;
	ControlRootImage next;
	ControlFileData thread;
	ClusterControlRootIdentity self;
	ClusterControlRootFileToken before, after;
	ClusterSharedConfigRef ref;
	ClusterSharedConfigImage image;
	ClusterSharedConfigStage stage;
	LOCKMODE cf_mode;
	uint64 epoch;
	uint64 incarnation;
	uint8 storage_uuid[16];
	bool changed;
} ConfigPublishWork;

static void
config_root_reference(const ControlRootImage *root, ClusterSharedConfigRef *ref)
{
	memset(ref, 0, sizeof(*ref));
	ref->identity.system_identifier = root->header.system_identifier;
	ref->identity.database_incarnation = root->header.v2.database_incarnation;
	ref->identity.generation = root->header.v2.config_generation;
	memcpy(ref->identity.storage_uuid, root->header.storage_uuid, 16);
	memcpy(ref->identity.authority_uuid, root->header.authority_uuid, 16);
	memcpy(ref->identity.configured, root->header.v2.configured, sizeof(ref->identity.configured));
	memcpy(ref->sha256, root->header.v2.config_sha256, 32);
}

static ClusterControlRootResult
config_publish_read(ConfigPublishWork *work, ControlRootImage *root,
					ClusterControlRootFileToken *token, bool initial)
{
	ClusterControlRootResult result;
	ClusterControlRootIdentity self;
	int node = cluster_node_id;

	if (!runtime_v2_owner_current(work->epoch, work->incarnation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = read_control_version(work->storage_uuid, GetSystemIdentifier(), root, &work->thread,
								  token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!root->present[node]
		|| root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		|| (root->header.v2.serving[node / 64] & (UINT64_C(1) << (node % 64))) == 0
		|| root->records[node].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	self = root->records[node].identity;
	if (self.origin_node_id != node || self.origin_thread_id != node + 1
		|| self.origin_owner_incarnation != work->incarnation
		|| (!initial && !cluster_control_root_identity_equal(&self, &work->self)))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	/* A pending initializer pins its config generation. Let its existing
	 * owner close it; do not invalidate that exact operation or shrink the
	 * scan to currently serving/ALIVE peers. Even another origin matters. */
	for (int i = 0; i < CLUSTER_CONTROL_ROOT_RECORD_COUNT; ++i)
		if (root->startup[i].generation != 0)
			return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = read_thread_version(&self, root, &work->thread, token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (work->thread.state != DB_IN_PRODUCTION)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (!runtime_v2_owner_current(work->epoch, work->incarnation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (initial)
		work->self = self;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: copy the selected online input while the caller pins CF. This
 * read deliberately does not depend on data/writer permission: application
 * must still be able to progress when a dependent data gate is held.
 * It neither applies settings nor releases the caller's CF responsibility.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct ConfigSelectedWork {
	ControlRootImage root;
	ControlFileData common;
	ClusterSharedConfigSelected selected;
	ClusterSharedConfigImage image;
} ConfigSelectedWork;

static ClusterControlRootResult
config_selected_read(const ClusterSharedConfigRef *prior, ConfigSelectedWork *work)
{
	ClusterSharedConfigIdentity identity;
	ClusterControlRootResult root_result, result;
	root_result = read_control_version(
		prior->identity.storage_uuid, prior->identity.system_identifier, &work->root, &work->common,
		&work->selected.root, CONTROL_ROOT_HEADER_VERSION_V3);
	if (root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& root_result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return root_result;
	/* Reading the selected configuration is also required before DATA opens.
	 * It grants no writer, recovery or serving permission. */
	if (work->root.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| (work->root.header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
			&& work->root.header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_CLOSED
			&& work->root.header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED))
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	config_root_reference(&work->root, &work->selected.ref);
	identity = work->selected.ref.identity;
	identity.generation = prior->identity.generation;
	if (memcmp(&identity, &prior->identity, sizeof(identity)) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (work->selected.ref.identity.generation < prior->identity.generation)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (work->selected.ref.identity.generation == prior->identity.generation
		&& memcmp(&work->selected.ref, prior, sizeof(*prior)) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	/* Read-control validates the full selected composition. Keep the same CF
	 * interval while retaining the config bytes, so neither GC nor a newer
	 * publisher can change this input between its selection and owned copy. */
	result = cluster_shared_config_read_locked(cluster_shared_data_dir, &work->selected.ref,
											   &work->image);
	return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? root_result : result;
}

ClusterControlRootResult
cluster_control_root_config_read_locked(const ClusterSharedConfigRef *prior,
										ClusterSharedConfigSelected *out,
										ClusterSharedConfigImage *image)
{
	MemoryContext caller = CurrentMemoryContext;
	MemoryContext context;
	ClusterSharedConfigSelected selected;
	ClusterSharedConfigImage copied = { 0 };
	ClusterControlRootResult result;
	if (history_ranges_overlap(prior, sizeof(*prior), out, sizeof(*out))
		|| history_ranges_overlap(prior, sizeof(*prior), image, sizeof(*image))
		|| history_ranges_overlap(out, sizeof(*out), image, sizeof(*image)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (image != NULL)
		memset(image, 0, sizeof(*image));
	if (prior == NULL || out == NULL || image == NULL || prior->identity.generation == 0)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	context = AllocSetContextCreate(caller, "selected configuration input", ALLOCSET_DEFAULT_SIZES);
	MemoryContextSwitchTo(context);
	PG_TRY();
	{
		ConfigSelectedWork *work = palloc0(sizeof(*work));
		result = config_selected_read(prior, work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
			/* Transfer only the successful owned bytes, never lower readers'
			 * temporary state. All fallible allocation precedes output. */
			copied.bytes = MemoryContextAlloc(caller, work->image.len + 1);
			copied.len = work->image.len;
			memcpy(copied.bytes, work->image.bytes, copied.len + 1);
			selected = work->selected;
		}
		cluster_shared_config_free(&work->image);
		pfree(work);
	}
	PG_CATCH();
	{
		/* Also owns scratch that a lower reader has not yet returned. */
		MemoryContextSwitchTo(caller);
		MemoryContextDelete(context);
		PG_RE_THROW();
	}
	PG_END_TRY();
	MemoryContextSwitchTo(caller);
	MemoryContextDelete(context);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
		*out = selected;
		*image = copied;
	}
	return result;
}

/* PGRAC: the control role retains its read until the original CF release.
 * Only owned value bytes survive between ticks; no CF authority is cached.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static const char config_aux_caller;

typedef struct ConfigAuxCut {
	uint64 epoch, generation, routing, incarnation;
	int32 master;
} ConfigAuxCut;

typedef struct ConfigAuxRead {
	bool pending;
	uint64 cookie;
	ConfigAuxCut cut;
	ClusterSharedConfigRef prior;
	ClusterSharedConfigSelected selected;
	ClusterSharedConfigImage image;
	ClusterControlRootResult result;
} ConfigAuxRead;

static ConfigAuxRead config_aux_read;

static bool
config_aux_cut(ConfigAuxCut *cut)
{
	const ClusterResId resid
		= { .type = CLUSTER_CF_RESID_TYPE, .lockmethodid = DEFAULT_LOCKMETHOD };
	memset(cut, 0, sizeof(*cut));
	cut->epoch = cluster_epoch_get_current();
	cut->generation = cluster_grd_redeclare_generation();
	cut->incarnation = cluster_qvotec_get_self_incarnation();
	cut->master = cluster_grd_lookup_master_gen(&resid, &cut->routing);
	return cut->epoch != 0 && cut->incarnation != 0 && cut->incarnation != UINT64_MAX
		   && cut->master >= 0 && cut->routing != 0 && cut->epoch == cluster_epoch_get_current();
}

static void
config_aux_clear(void)
{
	cluster_shared_config_free(&config_aux_read.image);
	memset(&config_aux_read, 0, sizeof(config_aux_read));
}

static ClusterControlRootResult
config_aux_finish(const ClusterSharedConfigRef *prior, ClusterSharedConfigSelected *out,
				  ClusterSharedConfigImage *image)
{
	ConfigAuxRead *pending = &config_aux_read;
	ConfigAuxCut now;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;

	cluster_cf_retirement_poll();
	if (!cluster_cf_release_completed(ShareLock, pending->cookie)) {
		if (cluster_cf_held_by(ShareLock, &config_aux_caller))
			return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		config_aux_clear(); /* A different owner's completion is not ours. */
		return result;
	}
	if (memcmp(prior, &pending->prior, sizeof(*prior)) == 0 && config_aux_cut(&now)
		&& memcmp(&now, &pending->cut, sizeof(now)) == 0) {
		result = pending->result;
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
			|| result == CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED) {
			*out = pending->selected;
			*image = pending->image;
			memset(&pending->image, 0, sizeof(pending->image)); /* Transfer ownership. */
		}
	}
	config_aux_clear();
	return result;
}

ClusterControlRootResult
cluster_control_root_config_poll(const ClusterSharedConfigRef *prior,
								 ClusterSharedConfigSelected *out, ClusterSharedConfigImage *image)
{
	MemoryContext caller = CurrentMemoryContext;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	ConfigAuxRead *pending = &config_aux_read;

	if (history_ranges_overlap(prior, sizeof(*prior), out, sizeof(*out))
		|| history_ranges_overlap(prior, sizeof(*prior), image, sizeof(*image))
		|| history_ranges_overlap(out, sizeof(*out), image, sizeof(*image)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (image != NULL)
		memset(image, 0, sizeof(*image));
	if (prior == NULL || out == NULL || image == NULL || prior->identity.generation == 0
		|| !cluster_shared_config || !cluster_enabled || MyBackendType != B_LMON)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	PG_TRY();
	{
		/* An acquisition invalidated by control reconstruction may already
		 * have been abandoned before any image existed. Keep that duty moving. */
		cluster_cf_retirement_poll();
		if (pending->pending)
			result = config_aux_finish(prior, out, image);
		else if (!cluster_cf_held(ExclusiveLock)
				 && (!cluster_cf_held(ShareLock)
					 || cluster_cf_acquire_pending_owned(ShareLock, &config_aux_caller))
				 && cluster_cf_lock_poll_owned(ShareLock, &config_aux_caller)) {
			pending->cookie = cluster_cf_owner_cookie(ShareLock);
			if (!cluster_cf_held_is_clusterwide(ShareLock) || pending->cookie == 0
				|| !config_aux_cut(&pending->cut)) {
				cluster_control_root_config_cancel();
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			} else {
				pending->prior = *prior;
				/* Only the returned image outlives this tick's context. The
				 * lower reader cleans all of its own scratch on success/ERROR. */
				MemoryContextSwitchTo(TopMemoryContext);
				pending->result = cluster_control_root_config_read_locked(prior, &pending->selected,
																		  &pending->image);
				MemoryContextSwitchTo(caller);
				pending->pending = true;
				(void)cluster_cf_unlock_owned(ShareLock, &config_aux_caller);
				result = config_aux_finish(prior, out, image);
			}
		}
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(caller);
		cluster_control_root_config_cancel();
		PG_RE_THROW();
	}
	PG_END_TRY();
	return result;
}

void
cluster_control_root_config_cancel(void)
{
	if (MyBackendType != B_LMON)
		return;
	config_aux_clear();
	if (cluster_cf_held_by(ShareLock, &config_aux_caller))
		(void)cluster_cf_unlock_owned(ShareLock, &config_aux_caller);
}

static ClusterControlRootResult
config_publish_cleanup(ConfigPublishWork *work, ClusterControlRootResult result)
{
	LOCKMODE mode = work->cf_mode;
	/* Release can consume caller cancellation while waiting for retirement.
	 * Retire private staging first, before that fallible wait loses the query
	 * context. No formal object or root can be removed by discard. */
	cluster_shared_config_free(&work->image);
	if (work->stage.state != 0) {
		ClusterControlRootResult cleanup
			= cluster_shared_config_discard(cluster_shared_data_dir, &work->stage);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cleanup;
	}
	work->cf_mode = NoLock;
	if (mode != NoLock && cluster_cf_held(mode))
		result = release_cf(mode, result);
	return result;
}

static ClusterControlRootResult
config_publish_work(ConfigPublishWork *work, const ClusterSharedConfigEntry *change,
					ClusterSharedConfigPolicyReport *report)
{
	ClusterControlRootResult result;
	ClusterControlRootFileToken observed;
	ClusterSharedConfigRef installed;
	uint8 uuid[16];

	work->cf_mode = ShareLock;
	/* An unsuccessful acquisition can still own a retiring request. Entry
	 * excludes pre-existing CF ownership, so cleanup owns exactly this debt. */
	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = config_publish_read(work, &work->base, &work->before, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	config_root_reference(&work->base, &work->ref);
	result = cluster_shared_config_read_locked(cluster_shared_data_dir, &work->ref, &work->image);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!pg_strong_random(uuid, sizeof(uuid)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	uuid[6] = (uuid[6] & 0x0f) | 0x40;
	uuid[8] = (uuid[8] & 0x3f) | 0x80;
	result = cluster_shared_config_prepare_change(cluster_shared_data_dir, work->image.bytes,
												  work->image.len, &work->ref, change, uuid,
												  &work->stage, &work->changed, report);
	cluster_shared_config_free(&work->image);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!runtime_v2_owner_current(work->epoch, work->incarnation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	work->cf_mode = ExclusiveLock;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = config_publish_read(work, &work->next, &observed, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!file_token_equal(&work->before, &observed))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (!work->changed) {
		work->after = observed;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	if (work->next.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	result = cluster_shared_config_install(cluster_shared_data_dir, &work->stage);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_shared_config_read_locked(cluster_shared_data_dir, &work->stage.ref,
											   &work->image);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	cluster_shared_config_free(&work->image);
	work->next.header.v2.config_generation = work->stage.ref.identity.generation;
	memcpy(work->next.header.v2.config_sha256, work->stage.ref.sha256, 32);
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = GetCurrentTimestamp();
	result = encode_extended_image(&work->next, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!runtime_v2_owner_current(work->epoch, work->incarnation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	/* Post-read, never rollback. A failed write or caller cancellation can
	 * leave the new root durable; its application no longer belongs to SQL. */
	result = config_publish_read(work, &work->base, &work->after, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(work->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	config_root_reference(&work->base, &installed);
	if (memcmp(&installed, &work->stage.ref, sizeof(installed)) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	work->ref = installed;
	result = cluster_cf_control_projection_write_locked(&work->thread);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& !runtime_v2_owner_current(work->epoch, work->incarnation))
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return result;
}

ClusterControlRootResult
cluster_control_root_config_change(const ClusterSharedConfigEntry *change,
								   ClusterSharedConfigPublication *out,
								   ClusterSharedConfigPolicyReport *report)
{
	ConfigPublishWork *work;
	ClusterControlRootResult result;
	uint8 uuid[16];
	uint64 epoch, incarnation;

	if (history_ranges_overlap(change, sizeof(*change), out, sizeof(*out))
		|| history_ranges_overlap(change, sizeof(*change), report, sizeof(*report))
		|| history_ranges_overlap(out, sizeof(*out), report, sizeof(*report)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (report != NULL)
		memset(report, 0, sizeof(*report));
	if (change == NULL || out == NULL || report == NULL || change->name == NULL
		|| MyBackendType != B_BACKEND || !enableFsync || !cluster_shared_config || !cluster_enabled
		|| !cluster_controlfile_shared_authority)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	epoch = cluster_epoch_get_current();
	incarnation = cluster_qvotec_get_self_incarnation();
	if (!runtime_v2_owner_current(epoch, incarnation) || !current_storage_uuid(uuid))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = storage_contract_check(uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	work->epoch = epoch;
	work->incarnation = incarnation;
	memcpy(work->storage_uuid, uuid, sizeof(uuid));
	PG_TRY();
	{
		result = config_publish_work(work, change, report);
		result = config_publish_cleanup(work, result);
	}
	PG_CATCH();
	{
		(void)config_publish_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		out->ref = work->ref;
		out->root = work->after;
		out->changed = work->changed;
	}
	pfree(work);
	return result;
}

typedef enum CheckpointV2Purpose {
	CHECKPOINT_V2_ONLINE,
	CHECKPOINT_V2_SHUTDOWN_EVIDENCE
} CheckpointV2Purpose;

typedef struct CheckpointV2Work {
	CheckpointV2Purpose purpose;
	uint16 format_version;
	ControlRootImage base;
	ControlRootImage next;
	ClusterWalHistoryImage *close_history;
	ClusterWalHistoryImage *checkpoint_history;
	ClusterWalHistoryImage *checkpoint_history_check;
	ClusterWalHistoryStage checkpoint_history_stage;
	ClusterWalStartupObservation terminal_obligations;
	bool checkpoint_history_changed;
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
	ClusterWalSourceRef source_ref;
	uint32 checkpoint_crc;
	/* Original physical responsibility, independent of the selected native redo. */
	XLogRecPtr retained_lower;
} CheckpointV2Work;

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
	if (work->wal_reader != NULL) {
		if (!checkpoint_v2_wal_paths_current(work, self))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		/* Drop all cached decoder pages while retaining the pinned files. */
		XLogReaderFree(work->wal_reader);
		work->wal_reader = NULL;
	} else {
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

/* Re-read native input at each publication cut. Live checkpoint callers also
 * revalidate their native flush frontier and writer owner. A shutdown input
 * must end at this exact record; later complete WAL invalidates a clean cut.
 * Neither a readable record nor a claim alone grants durability or ownership.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult
checkpoint_v2_input_observe(CheckpointV2Work *work, const ControlFileData *control,
							XLogRecPtr end, uint32 crc)
{
	ClusterWalThreadClaimV2 claim;
	ClusterControlRootResult result
		= cluster_wal_claim_v2_read(cluster_wal_threads_dir, &work->source_ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->purpose == CHECKPOINT_V2_SHUTDOWN_EVIDENCE) {
		ClusterWalTailObservation tail;
		result = cluster_wal_tail_observe_checkpoint(
			cluster_wal_threads_dir, &work->source_ref, wal_segment_size,
			work->retained_lower != InvalidXLogRecPtr ? work->retained_lower : control->checkPoint,
			end, control->checkPoint, crc, &tail);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		return tail.complete_end == end && tail.last_record_start == control->checkPoint
				   && tail.last_record_crc == crc
				   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (work->retained_lower != InvalidXLogRecPtr) {
		ClusterWalTailObservation prefix;
		result = cluster_wal_checkpoint_prefix_observe(cluster_wal_threads_dir, &work->source_ref,
													   wal_segment_size, work->retained_lower, end,
													   control->checkPoint, crc, &prefix);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = checkpoint_v2_wal_verify(work, &work->source_ref.claim.identity, control, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return work->checkpoint_crc == crc ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
									  : CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
}

static ClusterControlRootResult
checkpoint_v2_cleanup(CheckpointV2Work *work, ClusterControlRootResult result)
{
	ClusterControlRootResult discarded;

	if (work->close_history != NULL) {
		pfree(work->close_history);
		work->close_history = NULL;
	}
	if (work->checkpoint_history != NULL) {
		pfree(work->checkpoint_history);
		work->checkpoint_history = NULL;
	}
	if (work->checkpoint_history_check != NULL) {
		pfree(work->checkpoint_history_check);
		work->checkpoint_history_check = NULL;
	}
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
	if (work->checkpoint_history_stage.owner_pid != 0) {
		discarded = cluster_wal_history_discard(&work->checkpoint_history_stage);
		if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = discarded;
	}
	if (work->stage.owner_pid != 0) {
		discarded = cluster_recovery_anchor_v2_discard(&work->stage);
		if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				|| result == CLUSTER_CONTROL_ROOT_CAS_CONFLICT))
			result = discarded;
	}
	return result;
}

/* PGRAC: a clean restart reserves the complete declared roster in one CAS.
 * Prior-exit evidence is collected by existing LMON, not supplied as a boolean
 * by the caller. The old generations stay current and immutable; this grants
 * neither native mutation nor ordinary serving. Author: SqlRush <sqlrush@gmail.com> */
typedef struct ReserveCleanWork {
	ControlRootImage base, next;
	CheckpointV2Work scan;
	ClusterFormationSnapshotV1 formation;
	ClusterStartupExitCut cut;
	ClusterWalStartupImage operations[CLUSTER_MAX_NODES];
	ClusterWalStartupStage stages[CLUSTER_MAX_NODES];
	uint8 evidence[32];
	LOCKMODE cf_mode;
} ReserveCleanWork;

static bool
startup_owner_current(uint64 system_identifier, uint64 epoch, uint64 incarnation,
					  const uint64 required[2])
{
	ClusterFenceAuthorityProof authority;

	if (MyBackendType != B_STARTUP || ShutdownRequestPending || !enableFsync || !cluster_enabled
		|| !cluster_shared_config || !cluster_controlfile_shared_authority || system_identifier == 0
		|| GetSystemIdentifier() != system_identifier || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES || epoch == 0 || incarnation == 0
		|| cluster_qvotec_get_self_incarnation() != incarnation
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id) != incarnation
		|| !cluster_qvotec_in_quorum() || cluster_reconfig_has_pending_prebump_stage()
		|| !cluster_external_fence_runtime_active() || !cluster_write_fence_enforcing()
		|| !cluster_write_fence_allowed() || cluster_epoch_get_current() != epoch
		|| cluster_write_fence_read_durable_authority(&authority) != CLUSTER_FENCE_AUTHORITY_OK
		|| authority.marker.fence_epoch != epoch)
		return false;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node)
		if ((required[node / 64] & (UINT64_C(1) << (node % 64))) != 0
			&& cluster_fence_marker_node_is_fenced(authority.marker.fenced_dead_bitmap, node))
			return false;
	return cluster_epoch_get_current() == epoch && cluster_write_fence_allowed();
}

static bool
reserve_clean_owner_current(const ClusterStartupExitCut *cut)
{
	return cut->key.coordinator == (uint32)cluster_node_id
		   && startup_owner_current(cut->key.system_identifier, cut->key.epoch,
									cut->key.coordinator_incarnation, cut->required);
}

/* CF is held only for selection/reselection, never while decoding WAL. Full
 * formation equality includes the event identity, not just its MEMBER set. */
static ClusterControlRootResult
reserve_clean_reobserve(ReserveCleanWork *work, const ClusterStartupExitCut *expected, bool first)
{
	ClusterFormationSnapshotV1 formation;
	ClusterStartupExitCut cut;
	ClusterControlRootFileToken token;
	ControlFileData common;
	uint8 digest[32];
	ClusterControlRootResult result;

	if (!reserve_clean_owner_current(expected)
		|| !cluster_reconfig_capture_formation_snapshot_v1(cluster_node_id + 1, &formation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result
		= read_control_version(expected->key.storage_uuid, expected->key.system_identifier,
							   &work->scan.base, &common, &token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	result = cluster_control_root_v3_clean_exit_cut(
		work->scan.base.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES, expected->key.storage_uuid,
		expected->key.system_identifier, &formation, &cut);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&cut, expected, sizeof(cut)) != 0
		|| (!first
			&& (memcmp(&formation, &work->formation, sizeof(formation)) != 0
				|| memcmp(work->scan.base.bytes, work->base.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES)
					   != 0)))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (cluster_startup_exit_request(&cut, digest) != CLUSTER_STARTUP_EXIT_READY)
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (bytes_are_zero(digest, sizeof(digest)))
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	if (!first && memcmp(digest, work->evidence, sizeof(digest)) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (first) {
		work->base = work->scan.base;
		work->formation = formation;
		work->cut = cut;
		memcpy(work->evidence, digest, sizeof(digest));
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
reserve_clean_input_locked(ReserveCleanWork *work, unsigned node)
{
	CheckpointV2Work *scan = &work->scan;
	const ClusterControlRootSnapshot *record = &work->base.records[node];
	ClusterControlRootResult result
		= read_thread_input(&record->identity, &scan->base, &scan->old_view, &scan->before,
							CONTROL_ROOT_HEADER_VERSION_V3, true);

	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (memcmp(scan->base.bytes, work->base.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (scan->old_view.state != DB_SHUTDOWNED || record->checkpoint_lower_lsn == InvalidXLogRecPtr
		|| record->checkpoint_lower_lsn > scan->old_view.checkPoint
		|| scan->old_view.checkPointCopy.redo != scan->old_view.checkPoint
		|| record->tail_last_record_lsn != scan->old_view.checkPoint
		|| record->tail_last_record_crc32c != record->checkpoint_record_crc32c
		|| record->validated_tail_lsn_exclusive <= record->tail_last_record_lsn
		|| (record->root_flags
			& (CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
			   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID))
			   != (CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID
				   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID))
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	memset(&scan->source_ref, 0, sizeof(scan->source_ref));
	scan->source_ref.claim.identity = record->identity;
	scan->source_ref.claim.database_incarnation = work->base.header.v2.database_incarnation;
	scan->source_ref.claim.max_config_generation = work->base.header.v2.config_generation;
	memcpy(scan->source_ref.claim.claim_sha256, work->base.refs[node].claim_sha256, 32);
	scan->source_ref.timeline = record->checkpoint_tli;
	scan->retained_lower = record->checkpoint_lower_lsn;
	result
		= checkpoint_v2_input_observe(scan, &scan->old_view, record->validated_tail_lsn_exclusive,
									   record->checkpoint_record_crc32c);
	return result;
}

static ClusterControlRootResult
reserve_clean_operation(ReserveCleanWork *work, unsigned node)
{
	ClusterWalStartupImage *op = &work->operations[node];
	const ClusterControlRootSnapshot *record = &work->base.records[node];
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES];
	uint64 remainder;
	ClusterControlRootResult result;

	if (record->identity.root_lineage_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	op->phase = CLUSTER_WAL_STARTUP_RESERVED;
	op->input_kind = CLUSTER_WAL_STARTUP_CLEAN;
	if (!pg_strong_random(op->operation_uuid, sizeof(op->operation_uuid))
		|| bytes_are_zero(op->operation_uuid, sizeof(op->operation_uuid)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	op->database_incarnation = work->base.header.v2.database_incarnation;
	op->config_generation = work->base.header.v2.config_generation;
	op->formation_epoch = work->cut.key.epoch;
	op->predecessor_file_sequence = work->base.header.file_txn_seq;
	memcpy(op->predecessor_file_sha256, work->cut.key.root_sha256, 32);
	op->generation = work->base.header.file_txn_seq + 1;
	op->timeline = op->input_timeline = record->checkpoint_tli;
	op->segment_size = wal_segment_size;
	memcpy(op->predecessor_evidence_sha256, work->evidence, 32);
	op->input_record_start = record->tail_last_record_lsn;
	op->sealed_input_end = op->input_record_end = record->validated_tail_lsn_exclusive;
	op->input_record_crc = record->tail_last_record_crc32c;
	op->first_segment_lsn = op->sealed_input_end;
	remainder = op->first_segment_lsn % op->segment_size;
	if (remainder != 0) {
		if (op->first_segment_lsn > UINT64_MAX - (op->segment_size - remainder))
			return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
		op->first_segment_lsn += op->segment_size - remainder;
	}
	op->predecessor.snapshot = *record;
	op->predecessor.refs = work->base.refs[node];
	op->predecessor.publisher_incarnation = work->base.publisher_incarnation[node];
	op->predecessor.publisher_node = work->base.publisher_node[node];
	op->predecessor.record_crc32c = work->base.record_crc32c[node];
	op->claim.identity = record->identity;
	op->claim.identity.origin_owner_incarnation = work->cut.observer[node];
	op->claim.identity.root_lineage_seq++;
	op->claim.identity.thread_claim_created_at = GetCurrentTimestamp();
	op->claim.identity.thread_claim_crc32c = 0;
	op->claim.database_incarnation = op->database_incarnation;
	op->claim.config_generation = op->config_generation;
	op->claim.claim_generation = op->generation;
	result = cluster_wal_claim_v2_encode(&op->claim, claim);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		op->claim.identity.thread_claim_crc32c = read_u32_le(claim + 104);
	return result;
}

static ClusterControlRootResult
reserve_clean_cleanup(ReserveCleanWork *work, ClusterControlRootResult result)
{
	result = checkpoint_v2_cleanup(&work->scan, result);
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		if (work->stages[node].owner_pid != 0) {
			ClusterControlRootResult discarded = cluster_wal_startup_discard(&work->stages[node]);
			if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				result = discarded;
		}
	}
	if (work->cf_mode != NoLock) {
		LOCKMODE mode = work->cf_mode;
		work->cf_mode = NoLock;
		result = release_cf(mode, result);
	}
	return result;
}

static ClusterControlRootResult
reserve_clean_publish(ReserveCleanWork *work, const ClusterStartupExitCut *expected,
					  ClusterControlRootFileToken *out)
{
	ClusterControlRootResult result;
	ClusterFormationSnapshotV1 formation;
	ControlFileData common;
	ClusterControlRootFileToken selected;
	uint8 evidence[32];
	CheckpointV2Work *scan = &work->scan;

	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ShareLock;
	result = reserve_clean_reobserve(work, expected, true);
	work->cf_mode = NoLock;
	result = release_cf(ShareLock, result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		if ((work->cut.required[node / 64] & (UINT64_C(1) << (node % 64))) == 0)
			continue;
		make_read_token(&work->base, node + 1, CONTROL_ROOT_SOURCE_PRIMARY, &scan->thread_token);
		if (cluster_wal_retention_root_publish_begin_exact(&scan->thread_token, false, &scan->walr)
			!= CLUSTER_WAL_PIN_OK)
			return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		if (!acquire_clusterwide_cf(ShareLock))
			return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		scan->cf_mode = ShareLock;
		result = reserve_clean_input_locked(work, node);
		scan->cf_mode = NoLock;
		result = release_cf(ShareLock, result);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = checkpoint_v2_wal_verify(scan, &work->base.records[node].identity, &scan->old_view,
										  work->base.records[node].validated_tail_lsn_exclusive);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (scan->checkpoint_crc != work->base.records[node].checkpoint_record_crc32c
			|| !checkpoint_v2_wal_paths_current(scan, &work->base.records[node].identity))
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		result = checkpoint_v2_input_observe(
			scan, &scan->old_view, work->base.records[node].validated_tail_lsn_exclusive, scan->checkpoint_crc);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = checkpoint_v2_cleanup(scan, result);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = reserve_clean_operation(work, node);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ExclusiveLock;
	result = reserve_clean_reobserve(work, expected, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work->next = work->base;
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = GetCurrentTimestamp();
	work->next.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED;
	memset(work->next.header.v2.serving, 0, sizeof(work->next.header.v2.serving));
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		ClusterWalStartupImage readback;
		ClusterWalStartupStage *stage = &work->stages[node];
		if ((work->cut.required[node / 64] & (UINT64_C(1) << (node % 64))) == 0)
			continue;
		result = reserve_clean_input_locked(work, node);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = cluster_wal_startup_prepare(&work->next, node, &work->operations[node], stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = cluster_wal_startup_install(stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		work->next.startup[node].generation = stage->generation;
		memcpy(work->next.startup[node].sha256, stage->sha256, 32);
		result = cluster_wal_startup_read_locked(&work->next, node, &readback);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = cluster_control_root_v3_encode(&work->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = reserve_clean_reobserve(work, expected, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_control_version(expected->key.storage_uuid, expected->key.system_identifier,
								  &scan->base, &common, &selected, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(scan->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		ClusterWalStartupImage readback;
		if (work->next.startup[node].generation == 0)
			continue;
		result = cluster_wal_startup_read_locked(&scan->base, node, &readback);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (!reserve_clean_owner_current(expected)
		|| !cluster_reconfig_capture_formation_snapshot_v1(cluster_node_id + 1, &formation)
		|| memcmp(&formation, &work->formation, sizeof(formation)) != 0
		|| cluster_startup_exit_request(&work->cut, evidence) != CLUSTER_STARTUP_EXIT_READY
		|| memcmp(evidence, work->evidence, sizeof(evidence)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = selected;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v3_reserve_clean(const ClusterStartupExitCut *expected,
									  ClusterControlRootFileToken *out)
{
	ReserveCleanWork *work;
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(expected, sizeof(*expected), out, sizeof(*out));

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || expected == NULL || out == NULL || !IsValidWalSegSize(wal_segment_size))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held_is_clusterwide(ShareLock) || cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!reserve_clean_owner_current(expected))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = storage_contract_check(expected->key.storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	work->scan.purpose = CHECKPOINT_V2_SHUTDOWN_EVIDENCE;
	work->scan.format_version = CONTROL_ROOT_HEADER_VERSION_V3;
	for (unsigned i = 0; i < lengthof(work->scan.wal_dirs); ++i)
		work->scan.wal_dirs[i] = -1;
	for (unsigned i = 0; i < lengthof(work->scan.wal_segments); ++i)
		work->scan.wal_segments[i] = -1;
	PG_TRY();
	{
		result = reserve_clean_publish(work, expected, out);
	}
	PG_CATCH();
	{
		(void)reserve_clean_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		memset(out, 0, sizeof(*out));
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = reserve_clean_cleanup(work, result);
	pfree(work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

typedef struct StartupTargetWork {
	ControlRootImage base, observed;
	ClusterWalStartupImage op;
	ClusterFormationSnapshotV1 formation;
	bool cf_held;
} StartupTargetWork;

/* PGRAC: one native startup observation, never a transport loop under CF.
 * The existing coordinator reserves/begins the cohort; targets prepare only
 * their own namespace. Initializing output is intent for the native adapter,
 * not a general writer or serving capability. Author: SqlRush <sqlrush@gmail.com> */
typedef struct StartupAdvanceWork {
	ControlRootImage root;
	ClusterFormationSnapshotV1 formation;
	ClusterStartupExitCut cut;
	ClusterWalStartupImage op;
	ClusterControlRootFileToken token;
	bool cf_held;
} StartupAdvanceWork;

static ClusterControlRootResult
startup_advance_observe(StartupAdvanceWork *work, const ClusterWalSourceRef *restart)
{
	ControlFileData common;
	ClusterControlRootResult result;
	unsigned node = cluster_node_id;

	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_held = true;
	result = read_control_version(restart->claim.identity.storage_uuid,
								  restart->claim.identity.system_identifier, &work->root, &common,
								  &work->token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!work->root.present[node]
		|| work->root.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| (work->root.header.v2.configured[node / 64] & (UINT64_C(1) << (node % 64))) == 0
		|| !cluster_control_root_identity_equal(&restart->claim.identity,
												&work->root.records[node].identity)
		|| restart->claim.database_incarnation != work->root.header.v2.database_incarnation
		|| restart->claim.max_config_generation != work->root.header.v2.config_generation
		|| restart->timeline != work->root.records[node].checkpoint_tli
		|| memcmp(restart->claim.claim_sha256, work->root.refs[node].claim_sha256, 32) != 0
		|| !startup_owner_current(
			restart->claim.identity.system_identifier, cluster_epoch_get_current(),
			cluster_qvotec_get_self_incarnation(), work->root.header.v2.configured))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (work->root.header.v2.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED) {
		if (!cluster_reconfig_capture_formation_snapshot_v1(node + 1, &work->formation))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		return cluster_control_root_v3_clean_exit_cut(
			work->root.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES, restart->claim.identity.storage_uuid,
			restart->claim.identity.system_identifier, &work->formation, &work->cut);
	}
	if (work->root.header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| work->root.header.v2.serving[0] != 0 || work->root.header.v2.serving[1] != 0
		|| work->root.startup[node].generation == 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	result = cluster_wal_startup_read_locked(&work->root, node, &work->op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->op.input_kind != CLUSTER_WAL_STARTUP_CLEAN
		|| work->op.claim.identity.origin_owner_incarnation != cluster_qvotec_get_self_incarnation()
		|| work->op.formation_epoch != cluster_epoch_get_current())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
startup_advance_step(StartupAdvanceWork *work, const ClusterWalSourceRef *restart,
					 ClusterWalStartupImage *out)
{
	ClusterControlRootResult result = startup_advance_observe(work, restart);
	ClusterControlRootFileToken published;
	ClusterWalStartupImage prepared;
	unsigned coordinator = 0;

	if (work->cf_held) {
		work->cf_held = false;
		result = release_cf(ShareLock, result);
	}
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	while (
		coordinator < CLUSTER_MAX_NODES
		&& (work->root.header.v2.configured[coordinator / 64] & (UINT64_C(1) << (coordinator % 64)))
			   == 0)
		++coordinator;
	if (work->root.header.v2.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED) {
		if (coordinator != (unsigned)cluster_node_id)
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		result = cluster_control_root_v3_reserve_clean(&work->cut, &published);
		return result == CLUSTER_CONTROL_ROOT_OK_PRIMARY ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
														 : result;
	}
	if (work->op.phase == CLUSTER_WAL_STARTUP_RESERVED) {
		result = cluster_control_root_v3_startup_prepare_target(&work->op.claim.identity,
																work->op.operation_uuid, &prepared);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (coordinator != (unsigned)cluster_node_id)
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		result = cluster_control_root_v3_startup_begin_clean(
			&work->op.claim.identity, work->op.operation_uuid, &work->token, &published);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
		return result;
	}
	if (work->op.phase != CLUSTER_WAL_STARTUP_INITIALIZING)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	return cluster_control_root_v3_startup_read_writer(&work->op.claim.identity,
													   work->op.operation_uuid, out);
}

ClusterControlRootResult
cluster_control_root_v3_startup_advance_clean(const ClusterWalSourceRef *restart,
											  ClusterWalStartupImage *out)
{
	StartupAdvanceWork *work;
	ClusterControlRootResult result;
	ClusterWalSourceRef selected;
	bool alias = history_ranges_overlap(restart, sizeof(*restart), out, sizeof(*out));

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || out == NULL || restart == NULL || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES || CritSectionCount != 0
		|| MyBackendType != B_STARTUP || ShutdownRequestPending || !cluster_shared_config
		|| !cluster_enabled || !cluster_controlfile_shared_authority
		|| !cluster_wal_thread_restart_v2_ref(&selected)
		|| memcmp(&selected, restart, sizeof(selected)) != 0
		|| restart->claim.identity.origin_node_id != cluster_node_id
		|| restart->claim.identity.origin_thread_id != cluster_node_id + 1
		|| cluster_cf_held_is_clusterwide(ShareLock)
		|| cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		result = startup_advance_step(work, restart, out);
	}
	PG_CATCH();
	{
		if (work->cf_held) {
			work->cf_held = false;
			(void)release_cf(ShareLock, CLUSTER_CONTROL_ROOT_IO_ERROR);
		}
		memset(out, 0, sizeof(*out));
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	pfree(work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

typedef enum StartupTargetAction {
	STARTUP_TARGET_PREPARE,
	STARTUP_TARGET_READ,
	STARTUP_TARGET_ROUTE
} StartupTargetAction;

/* Every declared target must still have exactly its selected incarnation.
 * A missing member is never an excuse to shrink a clean-start operation. */
static ClusterControlRootResult
startup_operation_formation(const ControlRootImage *root, uint32 phase, bool allow_progress,
							ClusterFormationSnapshotV1 *formation)
{
	if (root->header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| root->header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| root->header.v2.serving[0] != 0 || root->header.v2.serving[1] != 0
		|| !cluster_reconfig_capture_formation_snapshot_v1(cluster_node_id + 1, formation))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (formation->local_epoch == 0 || formation->prebump_sync_active != 0
		|| formation->self_join_failed != 0
		|| !bytes_are_zero(formation->reserved, sizeof(formation->reserved))
		|| !bytes_are_zero(formation->pending_join_bitmap, sizeof(formation->pending_join_bitmap)))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		bool required = (root->header.v2.configured[node / 64] & (UINT64_C(1) << (node % 64))) != 0;
		bool member = formation->membership.membership_state[node] == CLUSTER_MEMBER_MEMBER;
		ClusterWalStartupImage op;
		ClusterControlRootResult result;
		if (required != member)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		if (!required)
			continue;
		if (((formation->excluded_bitmap[node / 8] | formation->clean_departed_bitmap[node / 8]
			  | formation->removed_bitmap[node / 8])
			 & (1u << (node % 8)))
			!= 0)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		/* An earlier peer may already have finished its native checkpoint.
		 * Do not require all executors to advance at exactly the same speed;
		 * its installed current must still be this formation's exact owner. */
		if (allow_progress && root->startup[node].generation == 0) {
			if (root->records[node].lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
				|| root->records[node].identity.origin_owner_incarnation
					   != formation->membership.last_admitted_incarnation[node])
				return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			continue;
		}
		result = cluster_wal_startup_read_locked(root, node, &op);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if ((op.phase != phase && !(allow_progress && op.phase == CLUSTER_WAL_STARTUP_DURABLE))
			|| op.input_kind != CLUSTER_WAL_STARTUP_CLEAN
			|| op.config_generation != root->header.v2.config_generation
			|| op.formation_epoch != formation->local_epoch
			|| op.claim.identity.origin_owner_incarnation
				   != formation->membership.last_admitted_incarnation[node])
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
startup_prepare_target_locked(StartupTargetWork *work, const ClusterControlRootIdentity *self,
							  const uint8 operation_uuid[16], StartupTargetAction action)
{
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterFormationSnapshotV1 formation;
	ClusterControlRootResult result;
	unsigned node = self->origin_node_id;
	bool writer = action != STARTUP_TARGET_PREPARE;
	uint32 phase = writer ? CLUSTER_WAL_STARTUP_INITIALIZING : CLUSTER_WAL_STARTUP_RESERVED;

	result = read_control_version(self->storage_uuid, self->system_identifier, &work->base, &common,
								  &token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	result = cluster_wal_startup_read_locked(&work->base, node, &work->op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->op.phase != phase
		|| !cluster_control_root_identity_equal(self, &work->op.claim.identity)
		|| memcmp(operation_uuid, work->op.operation_uuid, 16) != 0
		|| !startup_owner_current(self->system_identifier, work->op.formation_epoch,
								  self->origin_owner_incarnation, work->base.header.v2.configured))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = startup_operation_formation(&work->base, phase, writer, &work->formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_startup_empty_locked(&work->base, node, !writer, !writer);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (action == STARTUP_TARGET_ROUTE) {
		ClusterWalSourceRef restart;
		if (!cluster_wal_thread_restart_v2_ref(&restart))
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		result = cluster_wal_startup_route_locked(&work->base, node, DataDir, &restart);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = read_control_version(self->storage_uuid, self->system_identifier, &work->observed,
								  &common, &token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (memcmp(work->observed.bytes, work->base.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = startup_operation_formation(&work->observed, phase, writer, &formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&formation, &work->formation, sizeof(formation)) != 0
		|| !startup_owner_current(self->system_identifier, work->op.formation_epoch,
								  self->origin_owner_incarnation, work->base.header.v2.configured))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return cluster_wal_startup_empty_locked(&work->observed, node, false, false);
}

static ClusterControlRootResult
startup_target_read(const ClusterControlRootIdentity *self, const uint8 operation_uuid[16],
					ClusterWalStartupImage *out, StartupTargetAction action)
{
	StartupTargetWork *work;
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(self, sizeof(*self), out, sizeof(*out))
				 || history_ranges_overlap(operation_uuid, 16, out, sizeof(*out));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || self == NULL || operation_uuid == NULL || out == NULL
		|| self->origin_node_id >= CLUSTER_MAX_NODES || self->origin_node_id != cluster_node_id
		|| self->origin_thread_id != self->origin_node_id + 1 || bytes_are_zero(operation_uuid, 16))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held_is_clusterwide(ShareLock) || cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (CritSectionCount != 0 || MyBackendType != B_STARTUP || !cluster_enabled
		|| !cluster_shared_config || !cluster_controlfile_shared_authority || ShutdownRequestPending
		|| !enableFsync || self->system_identifier != GetSystemIdentifier()
		|| self->origin_owner_incarnation != cluster_qvotec_get_self_incarnation())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = storage_contract_check(self->storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		if (!acquire_clusterwide_cf(ExclusiveLock))
			result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else {
			work->cf_held = true;
			result = startup_prepare_target_locked(work, self, operation_uuid, action);
		}
	}
	PG_CATCH();
	{
		if (work->cf_held)
			(void)release_cf(ExclusiveLock, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (work->cf_held)
		result = release_cf(ExclusiveLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = work->op;
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_control_root_v3_startup_prepare_target(const ClusterControlRootIdentity *self,
											   const uint8 operation_uuid[16],
											   ClusterWalStartupImage *out)
{
	return startup_target_read(self, operation_uuid, out, STARTUP_TARGET_PREPARE);
}

ClusterControlRootResult
cluster_control_root_v3_startup_read_writer(const ClusterControlRootIdentity *self,
											const uint8 operation_uuid[16],
											ClusterWalStartupImage *out)
{
	return startup_target_read(self, operation_uuid, out, STARTUP_TARGET_READ);
}

ClusterControlRootResult
cluster_control_root_v3_startup_route_writer(const ClusterControlRootIdentity *self,
											 const uint8 operation_uuid[16],
											 ClusterWalStartupImage *out)
{
	return startup_target_read(self, operation_uuid, out, STARTUP_TARGET_ROUTE);
}

typedef struct StartupBeginWork {
	StartupTargetWork target;
	ControlRootImage next;
	ClusterWalStartupImage operations[CLUSTER_MAX_NODES];
	ClusterWalStartupStage stages[CLUSTER_MAX_NODES];
} StartupBeginWork;

static ClusterControlRootResult
startup_begin_cleanup(StartupBeginWork *work, ClusterControlRootResult result)
{
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		if (work->stages[node].owner_pid != 0) {
			ClusterControlRootResult discarded = cluster_wal_startup_discard(&work->stages[node]);
			if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				result = discarded;
		}
	}
	if (work->target.cf_held) {
		work->target.cf_held = false;
		result = release_cf(ExclusiveLock, result);
	}
	return result;
}

static ClusterControlRootResult
startup_begin_locked(StartupBeginWork *work, const ClusterControlRootIdentity *self,
					 const uint8 operation_uuid[16], const ClusterControlRootFileToken *expected,
					 ClusterControlRootFileToken *out)
{
	StartupTargetWork *target = &work->target;
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterFormationSnapshotV1 formation;
	ClusterControlRootResult result;
	unsigned own = self->origin_node_id;

	result = read_control_version(self->storage_uuid, self->system_identifier, &target->base,
								  &common, &token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!file_token_equal(expected, &token))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = cluster_wal_startup_read_locked(&target->base, own, &target->op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!cluster_control_root_identity_equal(self, &target->op.claim.identity)
		|| memcmp(operation_uuid, target->op.operation_uuid, 16) != 0
		|| !startup_owner_current(self->system_identifier, target->op.formation_epoch,
								  self->origin_owner_incarnation,
								  target->base.header.v2.configured))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = startup_operation_formation(&target->base, CLUSTER_WAL_STARTUP_RESERVED, false,
										 &target->formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (target->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	work->next = target->base;
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = GetCurrentTimestamp();
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		ClusterWalStartupImage *op = &work->operations[node];
		if ((target->base.header.v2.configured[node / 64] & (UINT64_C(1) << (node % 64))) == 0)
			continue;
		/* The already-existing lowest-MEMBER coordinator rule. */
		if (node < own)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		result = cluster_wal_startup_read_locked(&target->base, node, op);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (op->predecessor_file_sequence != target->op.predecessor_file_sequence
			|| memcmp(op->predecessor_file_sha256, target->op.predecessor_file_sha256, 32) != 0
			|| memcmp(op->predecessor_evidence_sha256, target->op.predecessor_evidence_sha256, 32)
				   != 0)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		/* A remote target may have failed after creating readable metadata
		 * but before its fsync. Reestablish durability ourselves before CAS. */
		result = cluster_wal_startup_empty_locked(&target->base, node, false, true);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		ClusterWalStartupImage *op = &work->operations[node];
		ClusterWalStartupStage *stage = &work->stages[node];
		ClusterWalStartupImage observed;
		if (op->phase == 0)
			continue;
		op->phase = CLUSTER_WAL_STARTUP_INITIALIZING;
		op->generation = work->next.header.file_txn_seq;
		result = cluster_wal_startup_prepare(&work->next, node, op, stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		result = cluster_wal_startup_install(stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		work->next.startup[node].generation = stage->generation;
		memcpy(work->next.startup[node].sha256, stage->sha256, 32);
		result = cluster_wal_startup_read_locked(&work->next, node, &observed);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = cluster_control_root_v3_encode(&work->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = read_control_version(self->storage_uuid, self->system_identifier, &target->observed,
								  &common, &token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!file_token_equal(expected, &token))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = startup_operation_formation(&target->observed, CLUSTER_WAL_STARTUP_RESERVED, false,
										 &formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&formation, &target->formation, sizeof(formation)) != 0
		|| !startup_owner_current(self->system_identifier, target->op.formation_epoch,
								  self->origin_owner_incarnation,
								  target->base.header.v2.configured))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	for (unsigned node = 0; node < CLUSTER_MAX_NODES; ++node) {
		if (work->operations[node].phase == 0)
			continue;
		result = cluster_wal_startup_empty_locked(&target->base, node, false, false);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (!publish_updated_image(&target->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_control_version(self->storage_uuid, self->system_identifier, &target->observed,
								  &common, &token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(target->observed.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = startup_operation_formation(&target->observed, CLUSTER_WAL_STARTUP_INITIALIZING, false,
										 &formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&formation, &target->formation, sizeof(formation)) != 0
		|| !startup_owner_current(self->system_identifier, target->op.formation_epoch,
								  self->origin_owner_incarnation,
								  target->base.header.v2.configured))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	*out = token;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_control_root_v3_startup_begin_clean(const ClusterControlRootIdentity *self,
											const uint8 operation_uuid[16],
											const ClusterControlRootFileToken *expected,
											ClusterControlRootFileToken *out)
{
	StartupBeginWork *work;
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(self, sizeof(*self), out, sizeof(*out))
				 || history_ranges_overlap(operation_uuid, 16, out, sizeof(*out))
				 || history_ranges_overlap(expected, sizeof(*expected), out, sizeof(*out));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || self == NULL || operation_uuid == NULL || expected == NULL || out == NULL
		|| self->origin_node_id >= CLUSTER_MAX_NODES || self->origin_node_id != cluster_node_id
		|| self->origin_thread_id != self->origin_node_id + 1 || bytes_are_zero(operation_uuid, 16))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held_is_clusterwide(ShareLock) || cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (MyBackendType != B_STARTUP || !cluster_enabled || !cluster_shared_config
		|| !cluster_controlfile_shared_authority || ShutdownRequestPending || !enableFsync
		|| self->system_identifier != GetSystemIdentifier()
		|| self->origin_owner_incarnation != cluster_qvotec_get_self_incarnation())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = storage_contract_check(self->storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		if (!acquire_clusterwide_cf(ExclusiveLock))
			result = CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
		else {
			work->target.cf_held = true;
			result = startup_begin_locked(work, self, operation_uuid, expected, out);
		}
	}
	PG_CATCH();
	{
		(void)startup_begin_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		memset(out, 0, sizeof(*out));
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = startup_begin_cleanup(work, result);
	pfree(work);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(out, 0, sizeof(*out));
	return result;
}

/* PGRAC: first native EOR checkpoint is a non-serving startup transition.
 * Reuse the physical checkpoint verifier, never ordinary serving
 * permission. The old current and history remain root-selected until INSTALL.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct StartupCheckpointWork {
	CheckpointV2Work scan;
	ControlRootImage observed;
	ClusterWalStartupImage op, durable;
	ClusterWalStartupStage operation_stage;
	ClusterFormationSnapshotV1 formation;
} StartupCheckpointWork;

static bool
startup_checkpoint_owner(const StartupCheckpointWork *work, const ClusterControlRootIdentity *self,
						 XLogRecPtr end)
{
	/* The dedicated EOR accessor observes native fsync before recovery DONE. */
	return GetXLogInsertEndRecPtr() == end
		   && ClusterXLogStartupFlushCovers(end, work->op.timeline)
		   && startup_owner_current(self->system_identifier, work->op.formation_epoch,
									self->origin_owner_incarnation,
									work->scan.base.header.v2.configured)
		   && cluster_wal_writer_startup_matches(self, work->op.operation_uuid,
												  work->op.first_segment_lsn);
}

static ClusterControlRootResult
startup_checkpoint_reobserve(StartupCheckpointWork *work, const ClusterControlRootIdentity *self,
							 bool published, const ControlFileData *cf, XLogRecPtr end)
{
	CheckpointV2Work *scan = &work->scan;
	ControlFileData common;
	ClusterControlRootFileToken token;
	ClusterFormationSnapshotV1 formation;
	ClusterWalThreadClaimV2 claim;
	ClusterControlRootResult result = read_control_version(
		self->storage_uuid, self->system_identifier, &work->observed, &common, &token, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (memcmp(work->observed.bytes, published ? scan->next.bytes : scan->base.bytes,
			   CLUSTER_CONTROL_ROOT_FILE_BYTES)
		!= 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = startup_operation_formation(&work->observed, CLUSTER_WAL_STARTUP_INITIALIZING, true,
										 &formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&formation, &work->formation, sizeof(formation)) != 0
		|| !startup_checkpoint_owner(work, self, end)
		|| !checkpoint_v2_wal_paths_current(scan, self))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &scan->source_ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return checkpoint_v2_input_observe(scan, cf, end, scan->checkpoint_crc);
}

static ClusterControlRootResult
startup_checkpoint_publish(StartupCheckpointWork *work, const ClusterControlRootIdentity *self,
						   const uint8 operation_uuid[16], const ControlFileData *cf,
						   XLogRecPtr end)
{
	CheckpointV2Work *scan = &work->scan;
	ClusterControlRootResult result;
	ClusterRecoveryAnchorV2 anchor = { 0 };
	ClusterRecoveryAnchorRefV2 anchor_ref = { 0 };
	ClusterWalThreadClaimV2 claim;
	ClusterWalPinResult pin;
	ClusterControlRootSnapshot *successor;
	uint8 encoded_claim[CLUSTER_WAL_CLAIM_V2_BYTES], uuid[16];
	unsigned node = self->origin_node_id;

	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	scan->cf_mode = ShareLock;
	result = read_control_version(self->storage_uuid, self->system_identifier, &scan->base,
								  &scan->old_view, &scan->before, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	result = cluster_wal_startup_read_locked(&scan->base, node, &work->op);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if ((work->op.phase != CLUSTER_WAL_STARTUP_INITIALIZING
		 && work->op.phase != CLUSTER_WAL_STARTUP_DURABLE)
		|| memcmp(operation_uuid, work->op.operation_uuid, 16) != 0
		|| !cluster_control_root_identity_equal(self, &work->op.claim.identity)
		|| work->op.segment_size != wal_segment_size || !startup_checkpoint_owner(work, self, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = startup_operation_formation(&scan->base, CLUSTER_WAL_STARTUP_INITIALIZING, true,
										 &work->formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* Re-read the actual predecessor claim/anchor, not a copy supplied by the
	 * startup caller. The thread-scoped WALR guard also pins the pending writer. */
	result = read_thread_version(&work->op.predecessor.snapshot.identity, &work->observed,
								 &scan->old_view, &scan->after, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!file_token_equal(&scan->before, &scan->after))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (cf->checkPointCopy.ThisTimeLineID != work->op.timeline
		|| cf->checkPointCopy.PrevTimeLineID != scan->old_view.checkPointCopy.PrevTimeLineID
		|| cf->checkPoint < work->op.first_segment_lsn + SizeOfXLogLongPHD
		|| cf->unloggedLSN < scan->old_view.unloggedLSN || cf->wal_level != scan->old_view.wal_level
		|| cf->wal_log_hints != scan->old_view.wal_log_hints
		|| cf->track_commit_timestamp != scan->old_view.track_commit_timestamp
		|| cf->checkPointCopy.fullPageWrites != scan->old_view.checkPointCopy.fullPageWrites)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (scan->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	make_read_token(&scan->base, self->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&scan->thread_token);
	scan->source_ref.claim.identity = *self;
	scan->source_ref.claim.database_incarnation = work->op.database_incarnation;
	scan->source_ref.claim.max_config_generation = work->op.config_generation;
	scan->source_ref.timeline = work->op.timeline;
	result = cluster_wal_claim_v2_encode(&work->op.claim, encoded_claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!control_root_sha256(encoded_claim, sizeof(encoded_claim),
							 scan->source_ref.claim.claim_sha256))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	scan->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	pin = cluster_wal_retention_root_publish_begin_exact(&scan->thread_token, false, &scan->walr);
	if (pin != CLUSTER_WAL_PIN_OK)
		return pin == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
											: CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &scan->source_ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = checkpoint_v2_wal_verify(scan, self, cf, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = checkpoint_v2_input_observe(scan, cf, end, scan->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	/* An uncertain root write may already have selected DURABLE. Only the same
	 * initialized process may reobserve it; a replacement must use recovery. */
	if (work->op.phase == CLUSTER_WAL_STARTUP_DURABLE) {
		if (work->op.successor.snapshot.checkpoint_lower_lsn != cf->checkPoint
			|| work->op.successor.snapshot.checkpoint_record_crc32c != scan->checkpoint_crc
			|| work->op.successor.snapshot.validated_tail_lsn_exclusive != end)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		anchor_ref.identity = *self;
		anchor_ref.database_incarnation = work->op.database_incarnation;
		anchor_ref.max_config_generation = work->op.config_generation;
		anchor_ref.anchor_generation = work->op.successor.refs.anchor_generation;
		memcpy(anchor_ref.anchor_sha256, work->op.successor.refs.anchor_sha256, 32);
		memcpy(anchor_ref.claim_sha256, scan->source_ref.claim.claim_sha256, 32);
		work->durable = work->op;
	} else {
		anchor.identity = *self;
		anchor.database_incarnation = work->op.database_incarnation;
		anchor.config_generation = work->op.config_generation;
		anchor.anchor_generation = scan->base.header.file_txn_seq + 1;
		memcpy(anchor.claim_sha256, scan->source_ref.claim.claim_sha256, 32);
		anchor.state = DB_SHUTDOWNED; /* native EOR record, not root clean-close */
		anchor.write_time = cf->time;
		anchor.checkpoint = cf->checkPoint;
		anchor.checkpoint_copy = cf->checkPointCopy;
		anchor.unlogged_lsn = cf->unloggedLSN;
		anchor.wal_log_hints = cf->wal_log_hints;
		anchor.track_commit_timestamp = cf->track_commit_timestamp;
		anchor.wal_level = cf->wal_level;
		anchor.max_connections = Max(cf->MaxConnections, scan->old_view.MaxConnections);
		anchor.max_worker_processes
			= Max(cf->max_worker_processes, scan->old_view.max_worker_processes);
		anchor.max_wal_senders = Max(cf->max_wal_senders, scan->old_view.max_wal_senders);
		anchor.max_prepared_xacts = Max(cf->max_prepared_xacts, scan->old_view.max_prepared_xacts);
		anchor.max_locks_per_xact = Max(cf->max_locks_per_xact, scan->old_view.max_locks_per_xact);
		if (!pg_strong_random(uuid, sizeof(uuid)))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		result = cluster_recovery_anchor_v2_prepare(&anchor, uuid, &scan->stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		anchor_ref = scan->stage.ref;
	}
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	scan->cf_mode = ExclusiveLock;
	result = startup_checkpoint_reobserve(work, self, false, cf, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (scan->stage.owner_pid != 0) {
		result = cluster_recovery_anchor_v2_install(&scan->stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = cluster_recovery_anchor_v2_read_locked(&anchor_ref, &scan->old_view, &scan->new_view);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (scan->new_view.state != DB_SHUTDOWNED || scan->new_view.checkPoint != cf->checkPoint
		|| !checkpoint_wal_matches(&scan->new_view.checkPointCopy, &cf->checkPointCopy)
		|| scan->new_view.minRecoveryPoint != 0 || scan->new_view.backupStartPoint != 0
		|| scan->new_view.backupEndPoint != 0 || scan->new_view.backupEndRequired)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (work->op.phase == CLUSTER_WAL_STARTUP_DURABLE) {
		char primary[MAXPGPATH];
		/* A failed durable_rename may leave the selected primary readable
		 * without a durable directory entry. Under the exact CF-X observation,
		 * repersist identical bytes and its parent, without advancing the root
		 * or replacing the predecessor backup. Readability alone is not ACK. */
		if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
			|| !write_durable_image(primary, scan->base.bytes))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		return startup_checkpoint_reobserve(work, self, false, cf, end);
	}
	scan->next = scan->base;
	scan->next.header.file_txn_seq++;
	scan->next.header.published_at_usec = GetCurrentTimestamp();
	work->durable = work->op;
	work->durable.phase = CLUSTER_WAL_STARTUP_DURABLE;
	work->durable.generation = scan->next.header.file_txn_seq;
	successor = &work->durable.successor.snapshot;
	successor->identity = *self;
	successor->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	successor->root_publish_seq = scan->next.header.file_txn_seq;
	successor->published_at_usec = scan->next.header.published_at_usec;
	successor->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN;
	successor->checkpoint_tli = successor->tail_tli = work->op.timeline;
	successor->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	successor->checkpoint_lower_lsn = cf->checkPoint;
	successor->checkpoint_record_crc32c = scan->checkpoint_crc;
	successor->root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	successor->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	successor->validated_tail_lsn_exclusive = end;
	successor->tail_last_record_lsn = cf->checkPoint;
	successor->tail_last_record_crc32c = scan->checkpoint_crc;
	work->durable.successor.publisher_node = node;
	work->durable.successor.publisher_incarnation = self->origin_owner_incarnation;
	work->durable.successor.refs.anchor_generation = anchor_ref.anchor_generation;
	memcpy(work->durable.successor.refs.anchor_sha256, anchor_ref.anchor_sha256, 32);
	memcpy(work->durable.successor.refs.claim_sha256, anchor_ref.claim_sha256, 32);
	result = cluster_wal_startup_prepare(&scan->next, node, &work->durable, &work->operation_stage);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_startup_install(&work->operation_stage);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	scan->next.startup[node].generation = work->operation_stage.generation;
	memcpy(scan->next.startup[node].sha256, work->operation_stage.sha256, 32);
	result = cluster_control_root_v3_encode(&scan->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_startup_read_locked(&scan->next, node, &work->durable);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = startup_checkpoint_reobserve(work, self, false, cf, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!publish_updated_image(&scan->base, &scan->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	return startup_checkpoint_reobserve(work, self, true, cf, end);
}

static ClusterControlRootResult
startup_checkpoint_cleanup(StartupCheckpointWork *work, ClusterControlRootResult result)
{
	if (work->operation_stage.owner_pid != 0) {
		ClusterControlRootResult discarded = cluster_wal_startup_discard(&work->operation_stage);
		if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = discarded;
	}
	return checkpoint_v2_cleanup(&work->scan, result);
}

ClusterControlRootResult
cluster_control_root_v3_startup_checkpoint(const ClusterControlRootIdentity *self,
										   const uint8 operation_uuid[16],
										   const ControlFileData *control, XLogRecPtr end,
										   ClusterWalStartupImage *out)
{
	StartupCheckpointWork *work;
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(self, sizeof(*self), out, sizeof(*out))
				 || history_ranges_overlap(operation_uuid, 16, out, sizeof(*out))
				 || history_ranges_overlap(control, sizeof(*control), out, sizeof(*out));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || self == NULL || operation_uuid == NULL || control == NULL || out == NULL
		|| bytes_are_zero(operation_uuid, 16) || self->origin_node_id >= CLUSTER_MAX_NODES
		|| self->origin_node_id != cluster_node_id || self->origin_thread_id != cluster_node_id + 1
		|| control->state != DB_SHUTDOWNED || control->checkPoint == 0
		|| control->checkPointCopy.redo != control->checkPoint || end <= control->checkPoint
		|| control->backupStartPoint != 0 || control->backupEndPoint != 0
		|| control->backupEndRequired || control->minRecoveryPoint != 0
		|| control->minRecoveryPointTLI != 0 || control->wal_level < WAL_LEVEL_MINIMAL
		|| control->wal_level > WAL_LEVEL_LOGICAL || control->MaxConnections <= 0
		|| control->max_worker_processes < 0 || control->max_wal_senders < 0
		|| control->max_prepared_xacts < 0 || control->max_locks_per_xact <= 0
		|| cluster_cf_classify_buffer((const char *)control, sizeof(*control),
									  self->system_identifier)
			   != CLUSTER_CF_VALID)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (MyBackendType != B_STARTUP || CritSectionCount != 0 || !cluster_enabled
		|| !cluster_shared_config || !cluster_controlfile_shared_authority || !enableFsync
		|| ShutdownRequestPending || self->system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = storage_contract_check(self->storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	work->scan.purpose = CHECKPOINT_V2_SHUTDOWN_EVIDENCE;
	for (unsigned i = 0; i < lengthof(work->scan.wal_dirs); ++i)
		work->scan.wal_dirs[i] = -1;
	for (unsigned i = 0; i < lengthof(work->scan.wal_segments); ++i)
		work->scan.wal_segments[i] = -1;
	PG_TRY();
	{
		result = startup_checkpoint_publish(work, self, operation_uuid, control, end);
	}
	PG_CATCH();
	{
		(void)startup_checkpoint_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = startup_checkpoint_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = work->durable;
	pfree(work);
	return result;
}

/* PGRAC: INSTALL changes selection, not the certified successor bytes. Keep
 * the complete flat predecessor union and zero serving in the same root CAS.
 * A caller's saved DURABLE image is retry intent, never publication authority.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct StartupInstallWork {
	StartupCheckpointWork checkpoint;
	ControlRootImage input_root;
	ClusterWalHistoryImage history, observed_history;
	ClusterWalHistoryStage history_stage;
	bool already_installed;
} StartupInstallWork;

static ClusterControlRootResult
startup_install_history(StartupInstallWork *work, unsigned node)
{
	StartupCheckpointWork *check = &work->checkpoint;
	CheckpointV2Work *scan = &check->scan;
	ClusterWalHistoryRecord predecessor = check->op.predecessor;
	ClusterControlRootResult result;
	uint32 position = 0;
	uint64 incarnation = predecessor.snapshot.identity.origin_owner_incarnation;

	if (!work->already_installed) {
		ClusterWalOriginInputs *inputs = palloc(sizeof(*inputs));
		result = cluster_wal_origin_inputs_read_locked(&work->input_root, node, inputs);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			if (!inputs->has_pending
				|| memcmp(&inputs->pending, &check->op, sizeof(check->op)) != 0)
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			else {
				work->history = inputs->history;
				predecessor = inputs->current;
			}
		}
		pfree(inputs);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	} else if (work->input_root.refs[node].history_generation != 0) {
		/* A completed retry proves the installed current/history below, not a
		 * no-longer-selected PGWG file that may be eligible for later GC. */
		result = cluster_wal_history_read_locked(&work->input_root, node, &work->history);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (work->history.count >= CLUSTER_WAL_HISTORY_MAX_RECORDS)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	while (position < work->history.count
		   && work->history.records[position].snapshot.identity.origin_owner_incarnation
				  < incarnation)
		++position;
	if (position < work->history.count
		&& work->history.records[position].snapshot.identity.origin_owner_incarnation
			   == incarnation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	/* Flatten only after retaining the entire selected old history. */
	predecessor.refs.history_generation = 0;
	memset(predecessor.refs.history_sha256, 0, 32);
	memmove(&work->history.records[position + 1], &work->history.records[position],
			(work->history.count - position) * sizeof(predecessor));
	work->history.records[position] = predecessor;
	++work->history.count;
	for (uint32 i = 0; i < work->history.count; ++i) {
		const ClusterWalHistoryRecord *record = &work->history.records[i];
		result = closed_v2_record_locked(&scan->base, &record->snapshot, &record->refs,
										 &scan->old_view);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
startup_install_history_matches(StartupInstallWork *work, const ControlRootImage *selected,
								unsigned node)
{
	uint8 *encoded = palloc(CLUSTER_WAL_HISTORY_MAX_BYTES);
	uint8 hash[32];
	size_t length;
	ClusterControlRootResult result
		= cluster_wal_history_read_locked(selected, node, &work->observed_history);
	/* A completed INSTALL retry must still consume the selected terminal
	 * files. Matching the manifest digest alone does not retain those inputs. */
	for (uint32 i = 0;
		 result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && i < work->observed_history.terminal_count;
		 i++) {
		ClusterWalTerminalImage terminal;
		result = cluster_wal_terminal_read_locked(selected, node, i, &terminal);
	}
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_control_root_v3_history_encode(selected, node, &work->history, encoded,
														&length);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (!control_root_sha256(encoded, length, hash))
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		else if (memcmp(hash, selected->refs[node].history_sha256, 32) != 0)
			result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	}
	pfree(encoded);
	return result;
}

static ClusterControlRootResult
startup_install_reobserve(StartupInstallWork *work, bool published)
{
	StartupCheckpointWork *check = &work->checkpoint;
	CheckpointV2Work *scan = &check->scan;
	ClusterRecoveryAnchorRefV2 anchor = { 0 };
	ControlFileData actual;
	ClusterControlRootResult result
		= startup_checkpoint_reobserve(check, &check->op.claim.identity, published, &scan->new_view,
									   check->op.successor.snapshot.validated_tail_lsn_exclusive);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	anchor.identity = check->op.claim.identity;
	anchor.database_incarnation = check->op.database_incarnation;
	anchor.max_config_generation = check->op.config_generation;
	anchor.anchor_generation = check->op.successor.refs.anchor_generation;
	memcpy(anchor.anchor_sha256, check->op.successor.refs.anchor_sha256, 32);
	memcpy(anchor.claim_sha256, check->op.successor.refs.claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_locked(&anchor, &scan->old_view, &actual);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return memcmp(&actual, &scan->new_view, sizeof(actual)) == 0
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
}

static ClusterControlRootResult
startup_install_publish(StartupInstallWork *work, const ClusterWalStartupImage *expected)
{
	StartupCheckpointWork *check = &work->checkpoint;
	CheckpointV2Work *scan = &check->scan;
	const ClusterControlRootIdentity *self = &expected->claim.identity;
	unsigned node = self->origin_node_id;
	ClusterControlRootResult result;
	ControlRootStartupRefV3 reference;
	ClusterRecoveryAnchorRefV2 anchor = { 0 };
	ClusterWalThreadClaimV2 claim;
	uint8 encoded[CLUSTER_WAL_STARTUP_BYTES], uuid[16];
	XLogRecPtr end = expected->successor.snapshot.validated_tail_lsn_exclusive;
	ClusterWalPinResult pin;

	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	scan->cf_mode = ShareLock;
	result = read_control_version(self->storage_uuid, self->system_identifier, &scan->base,
								  &scan->old_view, &scan->before, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	work->already_installed = scan->base.startup[node].generation == 0;
	/* This copy is used only to validate the caller's old operation encoding
	 * and read its immutable history. It is not a root or an authority grant. */
	work->input_root = scan->base;
	if (work->already_installed) {
		ControlRootRecordRefsV2 refs = scan->base.refs[node];
		refs.history_generation = 0;
		memset(refs.history_sha256, 0, 32);
		if (!scan->base.present[node]
			|| memcmp(&scan->base.records[node], &expected->successor.snapshot,
					  sizeof(expected->successor.snapshot))
				   != 0
			|| memcmp(&refs, &expected->successor.refs, sizeof(refs)) != 0
			|| scan->base.publisher_node[node] != expected->successor.publisher_node
			|| scan->base.publisher_incarnation[node] != expected->successor.publisher_incarnation)
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		work->input_root.records[node] = expected->predecessor.snapshot;
		work->input_root.refs[node] = expected->predecessor.refs;
		work->input_root.publisher_node[node] = expected->predecessor.publisher_node;
		work->input_root.publisher_incarnation[node] = expected->predecessor.publisher_incarnation;
	}
	result = cluster_control_root_v3_startup_encode(&work->input_root, node, expected, encoded,
													&reference);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!work->already_installed) {
		if (memcmp(&reference, &scan->base.startup[node], sizeof(reference)) != 0)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		result = cluster_wal_startup_read_locked(&scan->base, node, &check->op);
	} else {
		work->input_root.startup[node] = reference;
		result = cluster_control_root_v3_startup_decode(encoded, sizeof(encoded), &work->input_root,
														node, &check->op);
	}
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (check->op.config_generation != scan->base.header.v2.config_generation
		|| check->op.segment_size != wal_segment_size
		|| !startup_checkpoint_owner(check, self, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = startup_operation_formation(&scan->base, CLUSTER_WAL_STARTUP_INITIALIZING, true,
										 &check->formation);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = startup_install_history(work, node);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->already_installed) {
		result = startup_install_history_matches(work, &scan->base, node);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	} else if (scan->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;

	scan->source_ref.claim.identity = *self;
	scan->source_ref.claim.database_incarnation = check->op.database_incarnation;
	scan->source_ref.claim.max_config_generation = check->op.config_generation;
	memcpy(scan->source_ref.claim.claim_sha256, check->op.successor.refs.claim_sha256, 32);
	scan->source_ref.timeline = check->op.timeline;
	result = cluster_wal_claim_v2_read(cluster_wal_threads_dir, &scan->source_ref.claim, &claim);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	anchor.identity = *self;
	anchor.database_incarnation = check->op.database_incarnation;
	anchor.max_config_generation = check->op.config_generation;
	anchor.anchor_generation = check->op.successor.refs.anchor_generation;
	memcpy(anchor.anchor_sha256, check->op.successor.refs.anchor_sha256, 32);
	memcpy(anchor.claim_sha256, check->op.successor.refs.claim_sha256, 32);
	result = cluster_recovery_anchor_v2_read_locked(&anchor, &scan->old_view, &scan->new_view);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (scan->new_view.state != DB_SHUTDOWNED
		|| scan->new_view.checkPoint != check->op.successor.snapshot.checkpoint_lower_lsn
		|| scan->new_view.checkPointCopy.redo != scan->new_view.checkPoint
		|| scan->new_view.minRecoveryPoint != 0 || scan->new_view.backupStartPoint != 0
		|| scan->new_view.backupEndPoint != 0 || scan->new_view.backupEndRequired)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	make_read_token(&scan->base, self->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&scan->thread_token);
	scan->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	pin = cluster_wal_retention_root_publish_begin_exact(&scan->thread_token, false, &scan->walr);
	if (pin != CLUSTER_WAL_PIN_OK)
		return pin == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
											: CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = checkpoint_v2_wal_verify(scan, self, &scan->new_view, end);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (scan->checkpoint_crc != check->op.successor.snapshot.checkpoint_record_crc32c)
		return CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
	result = checkpoint_v2_input_observe(scan, &scan->new_view, end, scan->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	scan->next = scan->base;
	if (!work->already_installed) {
		scan->next.header.file_txn_seq++;
		scan->next.header.published_at_usec = GetCurrentTimestamp();
		scan->next.records[node] = check->op.successor.snapshot;
		scan->next.refs[node] = check->op.successor.refs;
		scan->next.publisher_node[node] = check->op.successor.publisher_node;
		scan->next.publisher_incarnation[node] = check->op.successor.publisher_incarnation;
		memset(&scan->next.startup[node], 0, sizeof(scan->next.startup[node]));
		if (!pg_strong_random(uuid, sizeof(uuid)))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		result = cluster_wal_history_prepare(&scan->next, node, &work->history,
											 scan->next.header.file_txn_seq, uuid,
											 &work->history_stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		scan->next.refs[node].history_generation = work->history_stage.generation;
		memcpy(scan->next.refs[node].history_sha256, work->history_stage.sha256, 32);
		result = cluster_control_root_v3_encode(&scan->next);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	scan->cf_mode = ExclusiveLock;
	result = startup_install_reobserve(work, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!work->already_installed) {
		result = cluster_wal_history_install(&work->history_stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	result = startup_install_history_matches(work, &scan->next, node);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = startup_install_reobserve(work, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->already_installed) {
		char primary[MAXPGPATH];
		if (!build_control_path(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH)
			|| !write_durable_image(primary, scan->base.bytes))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	} else if (!publish_updated_image(&scan->base, &scan->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = startup_install_reobserve(work, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	return startup_install_history_matches(work, &scan->next, node);
}

static ClusterControlRootResult
startup_install_cleanup(StartupInstallWork *work, ClusterControlRootResult result)
{
	if (work->history_stage.owner_pid != 0) {
		ClusterControlRootResult discarded = cluster_wal_history_discard(&work->history_stage);
		if (discarded != CLUSTER_CONTROL_ROOT_OK_PRIMARY
			&& result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = discarded;
	}
	return startup_checkpoint_cleanup(&work->checkpoint, result);
}

ClusterControlRootResult
cluster_control_root_v3_startup_install_writer(const ClusterWalStartupImage *expected,
											   ClusterWalSourceRef *out)
{
	StartupInstallWork *work;
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(expected, sizeof(*expected), out, sizeof(*out));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || expected == NULL || out == NULL || expected->phase != CLUSTER_WAL_STARTUP_DURABLE
		|| expected->input_kind != CLUSTER_WAL_STARTUP_CLEAN
		|| expected->claim.identity.origin_node_id != cluster_node_id
		|| expected->claim.identity.origin_node_id >= CLUSTER_MAX_NODES
		|| expected->claim.identity.origin_node_id < 0
		|| expected->claim.identity.origin_thread_id != cluster_node_id + 1)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (MyBackendType != B_STARTUP || CritSectionCount != 0 || !cluster_enabled
		|| !cluster_shared_config || !cluster_controlfile_shared_authority || !enableFsync
		|| ShutdownRequestPending
		|| expected->claim.identity.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = storage_contract_check(expected->claim.identity.storage_uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	work->checkpoint.scan.purpose = CHECKPOINT_V2_SHUTDOWN_EVIDENCE;
	for (unsigned i = 0; i < lengthof(work->checkpoint.scan.wal_dirs); ++i)
		work->checkpoint.scan.wal_dirs[i] = -1;
	for (unsigned i = 0; i < lengthof(work->checkpoint.scan.wal_segments); ++i)
		work->checkpoint.scan.wal_segments[i] = -1;
	PG_TRY();
	{
		result = startup_install_publish(work, expected);
	}
	PG_CATCH();
	{
		(void)startup_install_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = startup_install_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = work->checkpoint.scan.source_ref;
	pfree(work);
	return result;
}

/* PGRAC: a serving successor's later checkpoint discharges only typed
 * checkpoint-less inputs. No normal history is pruned and no WAL is deleted.
 * Transfer parameter floors/FPW evidence before deselecting their certificate,
 * with the new anchor and history selected by the same exact root CAS.
 * Author: SqlRush <sqlrush@gmail.com> */
static ClusterControlRootResult
checkpoint_terminal_collect(CheckpointV2Work *work, unsigned node)
{
	ClusterWalHistoryImage *history;
	ClusterWalStartupObservation *limits = &work->terminal_obligations;
	const ClusterControlRootIdentity *self = &work->base.records[node].identity;
	ClusterControlRootResult result;
	uint32 kept = 0;

	if (work->format_version != 3 || work->base.refs[node].history_generation == 0)
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	if (work->base.startup[node].generation != 0)
		return CLUSTER_CONTROL_ROOT_RECONFIG_WAIT;
	work->checkpoint_history = history = palloc0(sizeof(*history));
	result = cluster_wal_history_read_locked(&work->base, node, history);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	for (uint32 i = 0; i < history->terminal_count; i++) {
		ClusterWalTerminalImage terminal;
		const ClusterControlRootIdentity *failed;
		result = cluster_wal_terminal_read_locked(&work->base, node, i, &terminal);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		failed = &terminal.initialization.claim.identity;
		if (!rejoin_terminal_namespace_equal(self, failed)
			|| self->origin_owner_incarnation <= failed->origin_owner_incarnation
			|| self->root_lineage_seq < failed->root_lineage_seq) {
			history->terminals[kept++] = history->terminals[i];
			continue;
		}
		/* The selected decoder checked the preserved real predecessor and
		 * terminal-only whitelist. Its producer already closed native effects;
		 * a later serving writer proves rejoin, not elapsed time or W2 intent. */
#define TERMINAL_MAX(field) limits->field = Max(limits->field, terminal.observation.field)
		TERMINAL_MAX(max_connections);
		TERMINAL_MAX(max_worker_processes);
		TERMINAL_MAX(max_wal_senders);
		TERMINAL_MAX(max_prepared_xacts);
		TERMINAL_MAX(max_locks_per_xact);
#undef TERMINAL_MAX
		limits->fpw_disabled |= terminal.observation.fpw_disabled;
		work->checkpoint_history_changed = true;
	}
	memset(&history->terminals[kept], 0,
		   (history->terminal_count - kept) * sizeof(history->terminals[0]));
	history->terminal_count = kept;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
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
	result = read_thread_version(self, &work->base, &work->old_view, &work->before,
								 work->format_version);
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
		|| cf->checkPointCopy.redo <= work->old_view.checkPointCopy.redo
		|| cf->checkPoint <= work->old_view.checkPoint || end < record->validated_tail_lsn_exclusive
		|| cf->minRecoveryPoint != work->old_view.minRecoveryPoint
		|| cf->minRecoveryPointTLI != work->old_view.minRecoveryPointTLI
		|| cf->unloggedLSN < work->old_view.unloggedLSN || cf->wal_level != work->old_view.wal_level
		|| cf->wal_log_hints != work->old_view.wal_log_hints
		|| cf->track_commit_timestamp != work->old_view.track_commit_timestamp)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	/* A native checkpoint closes this writer's DATA/SIDE duty, not the
	 * other sources' ancestry. Until their exact dependency owner supplies
	 * a qualified advance, retain the old physical lower in v3. No scalar
	 * supplied by the checkpoint caller authorizes GC. */
	if (work->format_version >= 3)
		work->retained_lower = record->checkpoint_lower_lsn;
	result = checkpoint_terminal_collect(work, index);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work->source_ref.claim.identity = record->identity;
	work->source_ref.claim.database_incarnation = work->base.header.v2.database_incarnation;
	work->source_ref.claim.max_config_generation = work->base.header.v2.config_generation;
	memcpy(work->source_ref.claim.claim_sha256, work->base.refs[index].claim_sha256, 32);
	work->source_ref.timeline = record->checkpoint_tli;
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
#define TERMINAL_BOUND(field)                                                                      \
	anchor.field = Max(anchor.field, (int32)work->terminal_obligations.field)
	TERMINAL_BOUND(max_connections);
	TERMINAL_BOUND(max_worker_processes);
	TERMINAL_BOUND(max_wal_senders);
	TERMINAL_BOUND(max_prepared_xacts);
	TERMINAL_BOUND(max_locks_per_xact);
#undef TERMINAL_BOUND
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
	result = checkpoint_v2_input_observe(work, cf, end, crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ExclusiveLock;
	result = read_thread_version(self, &work->next, &work->new_view, &actual, work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !checkpoint_v2_owner_current(self, epoch, cf->checkPointCopy.ThisTimeLineID, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!file_token_equal(&work->before, &actual))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = checkpoint_v2_input_observe(work, cf, end, crc);
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
	record->checkpoint_lower_lsn = work->retained_lower != InvalidXLogRecPtr
									   ? work->retained_lower
									   : cf->checkPointCopy.redo;
	record->checkpoint_record_crc32c = crc;
	record->root_flags
		|= CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	if (work->terminal_obligations.fpw_disabled)
		record->root_flags |= CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
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
	if (work->checkpoint_history_changed) {
		result = cluster_wal_history_prepare(&work->next, index, work->checkpoint_history,
											 work->next.header.file_txn_seq, uuid,
											 &work->checkpoint_history_stage);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_history_install(&work->checkpoint_history_stage);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		work->next.refs[index].history_generation = work->checkpoint_history_stage.generation;
		memcpy(work->next.refs[index].history_sha256, work->checkpoint_history_stage.sha256, 32);
	}
	result = encode_extended_image(&work->next, work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !checkpoint_v2_owner_current(self, epoch, cf->checkPointCopy.ThisTimeLineID, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = checkpoint_v2_input_observe(work, cf, end, crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_thread_version(self, &work->base, &work->new_view, &work->after,
								 work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (memcmp(work->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	if (work->checkpoint_history_changed) {
		work->checkpoint_history_check = palloc0(sizeof(*work->checkpoint_history_check));
		result
			= cluster_wal_history_read_locked(&work->base, index, work->checkpoint_history_check);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		if (memcmp(work->checkpoint_history_check, work->checkpoint_history,
				   sizeof(*work->checkpoint_history))
			!= 0)
			return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	}
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !checkpoint_v2_owner_current(self, epoch, cf->checkPointCopy.ThisTimeLineID, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = checkpoint_v2_input_observe(work, cf, end, crc);
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
					  ControlFileData *out_control, uint16 version)
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
	work->format_version = version;
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
		if (version >= 3)
			cluster_wal_thread_checkpoint_observed_v1(out, out_control->checkPointCopy.redo);
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
								 out_token, out_control, 2);
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
								 checkpoint_end, out, out_token, out_control, 2);
}

/* PGRAC: failure input is a purpose-bound root transition, not a v1 patch
 * or permission inferred from a dead bitmap. All borrowed evidence remains
 * owned by the caller until our confirmed lock cleanup.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct FailureInputV2Work {
	uint16 format_version;
	ControlRootImage base;
	ControlRootImage next;
	ControlFileData control;
	ClusterControlRootReadToken token;
	ClusterWalRootPublishGuard *walr;
	ClusterRecoverySerialGuard serial;
	ClusterWalSourceRef source_ref;
	ClusterWalTailObservation tail;
	LOCKMODE cf_mode;
	int32 publisher_node;
	uint64 publisher_incarnation;
	/* PGRAC (S9P2-05): founder self-seal of this node's own crashed
	 * generation -- no external fence and no IR (self_predecessor_authorized). */
	bool self;
	uint64 self_min_dead_us;
	ClusterWalSourceRef self_restart;
} FailureInputV2Work;

/*
 * PGRAC (S9P2-05, PU-D-5): after every instance failed, the founder seals
 * its own previous generation without isolating that old incarnation
 * externally.  The evidence: this is the startup process of a postmaster
 * holding PostgreSQL's data-directory interlock; the quorum admitted this
 * boot's strictly newer incarnation of the node as a MEMBER; this boot's
 * pre-heartbeat voting snapshot shows the old incarnation on every disk with
 * a last heartbeat older than both the death threshold and the write lease;
 * and the bootstrap-validated restart input names exactly this root's claim.
 * Residual risk (same node id restarted elsewhere while the old instance,
 * partitioned, still writes) is a deployment limit.  Author: SqlRush
 * <sqlrush@gmail.com>
 */
static bool
self_predecessor_authorized(const ClusterRecoverySerialRequest *request,
							const FailureInputV2Work *work)
{
	ClusterQvotecPriorExitObservation death;
	ClusterWalSourceRef restart;
	uint64 self = cluster_qvotec_get_self_incarnation();
	int index = (int)request->duty.origin_thread_id - 1;

	if (MyBackendType != B_STARTUP || ShutdownRequestPending
		|| cluster_node_id != work->publisher_node || self == 0
		|| self != work->publisher_incarnation || request->duty.origin_node_id != cluster_node_id
		|| request->duty.origin_thread_id != (uint16)(cluster_node_id + 1)
		|| self <= request->duty.origin_owner_incarnation || !cluster_qvotec_in_quorum()
		|| cluster_membership_get_state(cluster_node_id) != CLUSTER_MEMBER_MEMBER
		|| cluster_membership_get_last_admitted_incarnation(cluster_node_id) != self
		|| !cluster_wal_thread_restart_v2_ref(&restart)
		|| memcmp(&restart, &work->self_restart, sizeof(restart)) != 0
		|| !cluster_control_root_identity_equal(&restart.claim.identity, &request->duty))
		return false;
	/* Once the root is read, the restart input is exactly its claim. */
	if (work->base.header.file_txn_seq != 0
		&& (memcmp(restart.claim.claim_sha256, work->base.refs[index].claim_sha256, 32) != 0
			|| restart.claim.database_incarnation != work->base.header.v2.database_incarnation
			|| restart.claim.max_config_generation != work->base.header.v2.config_generation
			|| restart.timeline != work->base.records[index].checkpoint_tli))
		return false;
	return cluster_qvotec_prior_death_observe(
		(uint32)cluster_node_id, request->duty.origin_owner_incarnation, self,
		(uint64)GetCurrentTimestamp(), work->self_min_dead_us, &death);
}

static bool
failure_v2_authorized(const ClusterRecoverySerialRequest *request, const FailureInputV2Work *work)
{
	ClusterRecoveryDutyDigest digest;
	PgracExternalFenceDenyReason reason;
	uint32 count;
	bool original_owner = false;

	if (work->self)
		return self_predecessor_authorized(request, work);
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

/* PGRAC: the initializer inspection owns no new authority. WALR and the
 * purpose-bound IR remain borrowed from the existing recovery worker. Never
 * scan WAL while holding CF, nor return a stale observation after releasing it.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct InitializerObserveWork {
	ControlRootImage root;
	ControlRootImage native_root;
	ClusterControlRecoverySubject subject;
	ClusterWalInitializerInput input;
	bool cf_held;
} InitializerObserveWork;

static bool
initializer_owner_current(ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin)
{
	ClusterRecoveryDutyDigest digest;
	uint32 count;
	bool original_owner = false;

	if (cluster_recovery_serial_initializer_revalidate(serial) != CLUSTER_RECOVERY_SERIAL_CURRENT
		|| cluster_wal_retention_pin_revalidate(pin) != CLUSTER_WAL_PIN_OK || cluster_node_id < 0
		|| cluster_node_id >= CLUSTER_MAX_NODES || cluster_qvotec_get_self_incarnation() == 0
		|| (cluster_node_id == serial->duty.origin_node_id
			&& cluster_qvotec_get_self_incarnation() == serial->duty.origin_owner_incarnation)
		|| !cluster_recovery_duty_digest_for_claim(&serial->duty, true, &digest))
		return false;
	count = cluster_external_fence_need_set_count(serial->fence_need_set);
	if (count == 0 || count > CLUSTER_MAX_NODES)
		return false;
	for (uint32 i = 0; i < count; i++) {
		const PgracExternalFenceNeedV1 *need
			= cluster_external_fence_need_set_at(serial->fence_need_set, i);
		if (need == NULL || need->system_identifier != serial->duty.system_identifier
			|| memcmp(&need->canonical_duty_digest, &digest, sizeof(digest)) != 0)
			return false;
		if (need->victim_node_id == serial->duty.origin_node_id
			&& need->victim_incarnation == serial->duty.origin_owner_incarnation)
			original_owner = true;
	}
	return original_owner;
}

static ClusterControlRootResult
initializer_observe_locked(ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
						   InitializerObserveWork *work)
{
	ClusterControlRootResult result;
	uint8 uuid[16];

	if (!initializer_owner_current(serial, pin))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!current_storage_uuid(uuid) || memcmp(uuid, serial->duty.storage_uuid, sizeof(uuid)) != 0)
		return CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED;
	result = storage_contract_check(uuid, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!cluster_cf_lock(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_held = true;
	if (!cluster_cf_held_is_clusterwide(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = read_recovery_subject_locked(serial->duty.origin_thread_id, uuid,
										  serial->duty.system_identifier, &serial->duty,
										  &work->root, &work->subject);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (work->subject.kind != CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER
		|| memcmp(&serial->pending, &work->subject.pending, sizeof(serial->pending)) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	return initializer_owner_current(serial, pin) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
												  : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
initializer_inspect(ClusterRecoverySerialGuard *serial, ClusterWalRetentionPin *pin,
					ClusterWalInitializerInput *out, bool native_census)
{
	InitializerObserveWork *work;
	ClusterWalSourceRef ref = { 0 };
	ClusterControlRootResult result;
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES];
	bool alias = history_ranges_overlap(serial, sizeof(*serial), out, sizeof(*out));

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (alias)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!cluster_shared_config || !cluster_enabled || !cluster_controlfile_shared_authority
		|| serial == NULL || pin == NULL || serial->mode != CLUSTER_RECOVERY_SERIAL_INITIALIZER
		|| !cluster_control_pending_token_matches(&serial->pending, &serial->duty)
		|| !cluster_recovery_duty_key_valid_for_claim(&serial->duty, true)
		|| serial->duty.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work = palloc0(sizeof(*work));
	PG_TRY();
	{
		result = initializer_observe_locked(serial, pin, work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = cluster_wal_startup_read_locked(&work->root, serial->duty.origin_node_id,
													 &work->input.startup);
		if (work->cf_held) {
			work->cf_held = false;
			result = release_cf(ShareLock, result);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			const ClusterWalStartupImage *op = &work->input.startup;
			ref.claim.identity = op->claim.identity;
			ref.claim.database_incarnation = op->database_incarnation;
			ref.claim.max_config_generation = op->config_generation;
			ref.timeline = op->timeline;
			result = cluster_wal_claim_v2_encode(&op->claim, claim);
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& !control_root_sha256(claim, sizeof(claim), ref.claim.claim_sha256))
				result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& !initializer_owner_current(serial, pin))
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				result
					= cluster_wal_startup_observe(cluster_wal_threads_dir, &ref, op->segment_size,
												  op->first_segment_lsn, &work->input.observation);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && native_census
			&& work->input.observation.checkpoint_records == 0
			&& work->input.observation.unsupported_records == 0) {
			ClusterControlRootFileToken file;
			ControlFileData *native = &work->input.native_input;
			result = initializer_observe_locked(serial, pin, work);
			if (result == 0)
				result = read_thread_input(&work->input.startup.predecessor.snapshot.identity,
										   &work->native_root, native, &file, 3, true);
			if (result == 0
				&& (memcmp(work->native_root.bytes, work->root.bytes,
						   CLUSTER_CONTROL_ROOT_FILE_BYTES)
						!= 0
					|| native->state != DB_SHUTDOWNED || native->minRecoveryPoint != 0
					|| native->backupStartPoint != 0 || native->backupEndPoint != 0
					|| native->backupEndRequired))
				result = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			if (work->cf_held) {
				work->cf_held = false;
				result = release_cf(ShareLock, result);
			}
			if (result == 0 && !initializer_owner_current(serial, pin))
				result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			if (result == 0)
				result = cluster_control_native_side_observe(cluster_shared_data_dir,
															 serial->duty.origin_node_id, native,
															 &work->input.native);
			if (result == 0)
				work->input.native_observed = true;
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = initializer_observe_locked(serial, pin, work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			ClusterWalStartupObservation current;
			const ClusterWalStartupImage *op = &work->input.startup;
			result = cluster_wal_startup_observe(cluster_wal_threads_dir, &ref, op->segment_size,
												op->first_segment_lsn, &current);
			if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
				&& memcmp(&current, &work->input.observation, sizeof(current))
					   != 0)
				result = CLUSTER_CONTROL_ROOT_COPY_DIVERGENT;
		}
	}
	PG_CATCH();
	{
		ClusterControlRootResult cleanup = CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (work->cf_held)
			cleanup = release_cf(ShareLock, cleanup);
		pfree(work);
		if (cleanup == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm initializer observation lock cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (work->cf_held)
		result = release_cf(ShareLock, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && !initializer_owner_current(serial, pin))
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = work->input;
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_control_root_v3_initializer_observe(ClusterRecoverySerialGuard *serial,
											ClusterWalRetentionPin *pin,
											ClusterWalInitializerInput *out)
{
	return initializer_inspect(serial, pin, out, false);
}

ClusterControlRootResult
cluster_control_root_v3_initializer_inspect(ClusterRecoverySerialGuard *serial,
											ClusterWalRetentionPin *pin,
											ClusterWalInitializerInput *out)
{
	return initializer_inspect(serial, pin, out, true);
}

/* PGRAC: sole-root checkpoint-less termination. This is the existing recovery
 * worker's executor, not an alternate authority. Old W0 stays current; actual
 * W1 inputs and the complete retained union survive. No native clean-bit or
 * elapsed time substitutes for isolation, durability or confirmed IR release.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct InitializerTerminalWork {
	ClusterRecoverySerialGuard *serial;
	ClusterWalRetentionPin *pin;
	ClusterWalRootPublishGuard *walr;
	ClusterWalInitializerInput input, checked;
	ControlRootImage base, next, observed;
	ClusterWalOriginInputs retained;
	ClusterWalTerminalImage terminal, readback;
	ClusterWalStartupStage terminal_stage;
	ClusterWalHistoryStage history_stage;
	ClusterRecoveryAnchorStageV2 anchor_stage;
	int32 publisher_node;
	uint64 publisher_incarnation;
	bool cf_held;
} InitializerTerminalWork;

static bool
initializer_terminal_current(const InitializerTerminalWork *work)
{
	return cluster_node_id == work->publisher_node
		   && cluster_qvotec_get_self_incarnation() == work->publisher_incarnation
		   && work->publisher_incarnation != 0
		   && (work->walr == NULL ? initializer_owner_current(work->serial, work->pin)
								  : cluster_wal_retention_pending_publish_current(
										work->walr, &work->serial->duty, &work->serial->pending));
}

static ClusterControlRootResult
initializer_terminal_read(InitializerTerminalWork *work, ControlRootImage *root)
{
	ClusterControlRecoverySubject subject;
	ClusterControlRootResult result;
	if (!initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_held = true;
	result = read_recovery_subject_locked(
		work->serial->duty.origin_thread_id, work->serial->duty.storage_uuid,
		work->serial->duty.system_identifier, &work->serial->duty, root, &subject);
	if (result != 0)
		return result;
	if (subject.kind != CLUSTER_CONTROL_RECOVERY_PENDING_INITIALIZER
		|| memcmp(&subject.pending, &work->serial->pending, sizeof(subject.pending)) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	return initializer_terminal_current(work) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
											  : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
initializer_terminal_digest(InitializerTerminalWork *work, const ClusterWalSourceRef *ref)
{
	ClusterWalTerminalImage *terminal = &work->terminal;
	const ClusterWalStartupObservation *o = &work->input.observation;
	const ClusterFenceAuthorityProof *formation
		= cluster_formation_witness_authority(work->serial->formation);
	/* Two 32-byte digests, three u64 authority values and two u32 disk
	 * counts follow the victim entries: the trailer is 96, not 64 bytes. */
	uint8 needs[4 + CLUSTER_MAX_NODES * 96 + 96] = { 0 };
	uint8 closure[384] = { 0 };
	uint32 count = cluster_external_fence_need_set_count(work->serial->fence_need_set);
	if (formation == NULL || formation->marker.fence_epoch == 0 || count == 0
		|| count > CLUSTER_MAX_NODES || work->serial->lock_request.request_id == 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	terminal->formation_epoch = formation->marker.fence_epoch;
	terminal->ir_request_id = work->serial->lock_request.request_id;
	write_u32_le(needs, count);
	for (uint32 i = 0; i < count; i++) {
		const PgracExternalFenceNeedV1 *need
			= cluster_external_fence_need_set_at(work->serial->fence_need_set, i);
		uint8 *p = needs + 4 + i * 96;
		if (need == NULL)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		write_u64_le(p, need->system_identifier);
		memcpy(p + 8, need->canonical_duty_digest.bytes, 32);
		write_u32_le(p + 40, need->victim_node_id);
		write_u64_le(p + 48, need->victim_incarnation);
		memcpy(p + 56, need->protected_set_digest, 32);
		write_u32_le(p + 88, need->predicate_id);
		write_u32_le(p + 92, need->predicate_version);
	}
	{
		const ClusterFenceAuthorityProof *authority = formation;
		const PgracExternalFenceWriterSetDigest *need_digest
			= cluster_external_fence_need_set_digest(work->serial->fence_need_set);
		const PgracExternalFenceWriterSetDigest *admission_digest
			= cluster_external_fence_admission_set_digest(work->serial->fence_admission_set);
		uint32 offset = 4 + count * 96;
		if (need_digest == NULL || admission_digest == NULL)
			return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		memcpy(needs + offset, need_digest->bytes, sizeof(need_digest->bytes));
		offset += sizeof(need_digest->bytes);
		memcpy(needs + offset, admission_digest->bytes, sizeof(admission_digest->bytes));
		offset += sizeof(admission_digest->bytes);
		write_u64_le(needs + offset, authority->marker.fence_epoch);
		offset += 8;
		write_u64_le(needs + offset, authority->marker.fence_event_id);
		offset += 8;
		write_u64_le(needs + offset, authority->marker.fence_generation);
		offset += 8;
		write_u32_le(needs + offset, authority->agree_disk_count);
		offset += 4;
		write_u32_le(needs + offset, authority->total_disk_count);
		Assert(offset + 4 <= sizeof(needs));
		if (!control_root_sha256(needs, offset + 4, terminal->isolation_sha256))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	memcpy(closure, "PGNC", 4);
	write_u32_le(closure + 4, 1);
	memcpy(closure + 8, terminal->sealing_sha256, 32);
	memcpy(closure + 40, terminal->original_ref.sha256, 32);
	memcpy(closure + 72, work->input.native.sha256, 32);
	memcpy(closure + 104, terminal->isolation_sha256, 32);
	memcpy(closure + 136, ref->claim.claim_sha256, 32);
	write_u64_le(closure + 168, o->tail.complete_end);
	write_u64_le(closure + 176, o->tail.last_record_start);
	write_u32_le(closure + 184, o->tail.last_record_crc);
	write_u64_le(closure + 192, o->tail.records);
	write_u64_le(closure + 200, o->fpw_records);
	write_u64_le(closure + 208, o->parameter_records);
	write_u64_le(closure + 216, work->input.native.effective_next_xid);
	write_u32_le(closure + 224, work->input.native.page_reads);
	write_u32_le(closure + 228, o->max_connections);
	write_u32_le(closure + 232, o->max_worker_processes);
	write_u32_le(closure + 236, o->max_wal_senders);
	write_u32_le(closure + 240, o->max_prepared_xacts);
	write_u32_le(closure + 244, o->max_locks_per_xact);
	write_u32_le(closure + 248, o->fpw_disabled);
	write_u64_le(closure + 256, terminal->formation_epoch);
	write_u64_le(closure + 264, terminal->ir_request_id);
	write_u32_le(closure + 272, terminal->recoverer_node);
	write_u64_le(closure + 280, terminal->recoverer_incarnation);
	return control_root_sha256(closure, sizeof(closure), terminal->closure_sha256)
			   ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
			   : CLUSTER_CONTROL_ROOT_IO_ERROR;
}

static ClusterControlRootResult
initializer_checkpoint_anchor(InitializerTerminalWork *work, const ClusterWalSourceRef *ref)
{
	const ClusterWalStartupImage *op = &work->input.startup;
	const ClusterWalStartupObservation *o = &work->input.observation;
	ClusterRecoveryAnchorV2 anchor = { 0 };
	ClusterControlRootFileToken file;
	ControlFileData input;
	ClusterControlRootResult result;
	uint8 uuid[16];

	/* Read only W0's non-checkpoint fields. W1's checkpoint is always taken
	 * from the physically decoded stream, including before DURABLE selection. */
	result = read_thread_input(&op->predecessor.snapshot.identity, &work->observed, &input, &file,
							   3, true);
	if (result != 0)
		return result;
	if (memcmp(work->base.bytes, work->observed.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (input.minRecoveryPoint != 0 || input.backupStartPoint != 0 || input.backupEndPoint != 0
		|| input.backupEndRequired || o->checkpoint.redo < op->first_segment_lsn + SizeOfXLogLongPHD
		|| o->checkpoint_end > o->tail.complete_end)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	anchor.identity = op->claim.identity;
	anchor.database_incarnation = op->database_incarnation;
	anchor.config_generation = op->config_generation;
	anchor.anchor_generation = work->base.header.file_txn_seq + 1;
	memcpy(anchor.claim_sha256, ref->claim.claim_sha256, 32);
	anchor.state = DB_IN_CRASH_RECOVERY;
	anchor.checkpoint = o->checkpoint_start;
	anchor.checkpoint_copy = o->checkpoint;
	anchor.write_time = o->checkpoint.time;
	anchor.unlogged_lsn = input.unloggedLSN;
	anchor.wal_log_hints = input.wal_log_hints;
	anchor.track_commit_timestamp = input.track_commit_timestamp;
	anchor.wal_level = input.wal_level;
	anchor.max_connections = Max(input.MaxConnections, o->max_connections);
	anchor.max_worker_processes = Max(input.max_worker_processes, o->max_worker_processes);
	anchor.max_wal_senders = Max(input.max_wal_senders, o->max_wal_senders);
	anchor.max_prepared_xacts = Max(input.max_prepared_xacts, o->max_prepared_xacts);
	anchor.max_locks_per_xact = Max(input.max_locks_per_xact, o->max_locks_per_xact);
	if (!pg_strong_random(uuid, sizeof(uuid)))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = cluster_recovery_anchor_v2_prepare(&anchor, uuid, &work->anchor_stage);
	if (result == 0)
		result = cluster_recovery_anchor_v2_install(&work->anchor_stage);
	return result;
}

static ClusterControlRootResult
initializer_checkpoint_history(InitializerTerminalWork *work)
{
	ClusterWalHistoryImage *history = &work->retained.history;
	ClusterWalHistoryRecord predecessor = work->retained.current;
	uint64 incarnation = predecessor.snapshot.identity.origin_owner_incarnation;
	uint32 position = 0;

	if (history->count + history->terminal_count >= CLUSTER_WAL_HISTORY_MAX_RECORDS)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	while (position < history->count
		   && history->records[position].snapshot.identity.origin_owner_incarnation < incarnation)
		position++;
	if (position < history->count
		&& history->records[position].snapshot.identity.origin_owner_incarnation == incarnation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	predecessor.refs.history_generation = 0;
	memset(predecessor.refs.history_sha256, 0, 32);
	memmove(&history->records[position + 1], &history->records[position],
			(history->count - position) * sizeof(predecessor));
	history->records[position] = predecessor;
	history->count++;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
initializer_checkpoint_stage(InitializerTerminalWork *work, const ClusterWalSourceRef *ref)
{
	const ClusterWalStartupObservation *o = &work->input.observation;
	uint32 node = work->serial->duty.origin_node_id;
	ClusterControlRootSnapshot *record;
	ClusterControlRootResult result;

	result = initializer_terminal_read(work, &work->base);
	if (result != 0)
		return result;
	if (work->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	result = cluster_wal_origin_inputs_read_locked(&work->base, node, &work->retained);
	if (result == 0)
		result = initializer_checkpoint_history(work);
	if (result == 0)
		result = initializer_checkpoint_anchor(work, ref);
	if (result != 0)
		return result;
	work->next = work->base;
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = GetCurrentTimestamp();
	record = &work->next.records[node];
	memset(record, 0, sizeof(*record));
	record->identity = work->serial->duty;
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	record->root_publish_seq = work->next.header.file_txn_seq;
	record->published_at_usec = work->next.header.published_at_usec;
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_FAILURE_TAIL_VALIDATED;
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	record->checkpoint_tli = record->tail_tli = record->recovered_tli = ref->timeline;
	record->checkpoint_lower_lsn = o->checkpoint.redo;
	record->checkpoint_record_crc32c = o->checkpoint_crc;
	record->root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	if (o->fpw_disabled || !o->checkpoint.fullPageWrites)
		record->root_flags |= CLUSTER_CONTROL_ROOT_FLAG_FPW_WAS_OFF;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->validated_tail_lsn_exclusive = o->tail.complete_end;
	record->tail_last_record_lsn = o->tail.last_record_start;
	record->tail_last_record_crc32c = o->tail.last_record_crc;
	/* The empty replay frontier is not RECOVERY_COMPLETE. */
	record->recovered_through_lsn_exclusive = o->checkpoint.redo;
	memset(&work->next.refs[node], 0, sizeof(work->next.refs[node]));
	work->next.refs[node].anchor_generation = work->anchor_stage.ref.anchor_generation;
	memcpy(work->next.refs[node].anchor_sha256, work->anchor_stage.ref.anchor_sha256, 32);
	memcpy(work->next.refs[node].claim_sha256, ref->claim.claim_sha256, 32);
	work->next.publisher_node[node] = work->publisher_node;
	work->next.publisher_incarnation[node] = work->publisher_incarnation;
	memset(&work->next.startup[node], 0, sizeof(work->next.startup[node]));
	work->next.header.v2.serving[node / 64] &= ~(UINT64_C(1) << (node % 64));
	result = cluster_wal_history_prepare(&work->next, node, &work->retained.history,
										 work->next.header.file_txn_seq,
										 work->input.startup.operation_uuid, &work->history_stage);
	if (result == 0)
		result = cluster_wal_history_install(&work->history_stage);
	if (result != 0)
		return result;
	work->next.refs[node].history_generation = work->history_stage.generation;
	memcpy(work->next.refs[node].history_sha256, work->history_stage.sha256, 32);
	return cluster_control_root_v3_encode(&work->next);
}

/* Promote only a physically sealed W1. The existing coordinator rediscovers
 * its ordinary recovery duty after our pending guard is retired; the worker
 * must not mark a promoted but unreplayed stream DONE. */
static ClusterControlRootResult
initializer_checkpoint_execute(InitializerTerminalWork *work)
{
	ClusterControlRootResult result;
	ClusterWalSourceRef ref = { 0 };
	ClusterWalStartupObservation synced;
	ClusterControlRootFileToken file;
	ControlFileData control;
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES];

	ref.claim.identity = work->input.startup.claim.identity;
	ref.claim.database_incarnation = work->input.startup.database_incarnation;
	ref.claim.max_config_generation = work->input.startup.config_generation;
	ref.timeline = work->input.startup.timeline;
	result = cluster_wal_claim_v2_encode(&work->input.startup.claim, claim);
	if (result != 0)
		return result;
	if (!control_root_sha256(claim, sizeof(claim), ref.claim.claim_sha256))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (!initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result
		= cluster_wal_startup_sync(cluster_wal_threads_dir, &ref, work->input.startup.segment_size,
								   work->input.startup.first_segment_lsn, &synced);
	if (result != 0)
		return result;
	if (memcmp(&synced, &work->input.observation, sizeof(synced)) != 0
		|| !initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = initializer_checkpoint_stage(work, &ref);
	if (result != 0)
		return result;
	work->cf_held = false;
	result = release_cf(ExclusiveLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != 0)
		return result;
	result = cluster_control_root_v3_initializer_inspect(work->serial, work->pin, &work->checked);
	if (result != 0)
		return result;
	if (memcmp(&work->input, &work->checked, sizeof(work->input)) != 0
		|| !initializer_terminal_current(work)
		|| cluster_wal_retention_pin_seal_for_root_publish(work->pin) != CLUSTER_WAL_PIN_OK)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_recovery_serial_release(work->serial) != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	if (cluster_wal_retention_pending_publish_begin(&work->serial->duty, &work->serial->pending,
													&work->walr)
		!= CLUSTER_WAL_PIN_OK)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = initializer_terminal_read(work, &work->observed);
	if (result != 0)
		return result;
	if (memcmp(work->base.bytes, work->observed.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (!initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_thread_input(&work->serial->duty, &work->observed, &control, &file, 3, true);
	if (result != 0)
		return result;
	if (memcmp(work->observed.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	result = cluster_wal_origin_inputs_read_locked(
		&work->observed, work->serial->duty.origin_node_id, &work->retained);
	if (result != 0)
		return result;
	return initializer_terminal_current(work) ? CLUSTER_CONTROL_ROOT_RECONFIG_WAIT
											  : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
initializer_terminal_execute(InitializerTerminalWork *work)
{
	ClusterControlRootResult result;
	ClusterWalSourceRef ref = { 0 };
	ClusterWalStartupObservation synced;
	ClusterNativeSideObservation native;
	ClusterControlRootFileToken file;
	ControlFileData control;
	ClusterWalTerminalImage *terminal = &work->terminal;
	ClusterWalHistoryImage *history = &work->retained.history;
	uint8 claim[CLUSTER_WAL_CLAIM_V2_BYTES];
	uint32 node = work->serial->duty.origin_node_id, position = 0;
	result = cluster_control_root_v3_initializer_inspect(work->serial, work->pin, &work->input);
	if (result != 0)
		return result;
	if (work->input.observation.checkpoint_records != 0)
		return initializer_checkpoint_execute(work);
	if (!work->input.native_observed || work->input.observation.unsupported_records != 0)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	ref.claim.identity = work->input.startup.claim.identity;
	ref.claim.database_incarnation = work->input.startup.database_incarnation;
	ref.claim.max_config_generation = work->input.startup.config_generation;
	ref.timeline = work->input.startup.timeline;
	result = cluster_wal_claim_v2_encode(&work->input.startup.claim, claim);
	if (result != 0)
		return result;
	if (!control_root_sha256(claim, sizeof(claim), ref.claim.claim_sha256))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (!initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result
		= cluster_wal_startup_sync(cluster_wal_threads_dir, &ref, work->input.startup.segment_size,
								   work->input.startup.first_segment_lsn, &synced);
	if (result != 0)
		return result;
	if (memcmp(&synced, &work->input.observation, sizeof(synced)) != 0
		|| !initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = cluster_control_native_side_sync(cluster_shared_data_dir, node,
											  &work->input.native_input, &native);
	if (result != 0)
		return result;
	if (memcmp(&native, &work->input.native, sizeof(native)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = cluster_control_root_v3_initializer_inspect(work->serial, work->pin, &work->checked);
	if (result != 0)
		return result;
	if (memcmp(&work->input, &work->checked, sizeof(work->input)) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = initializer_terminal_read(work, &work->base);
	if (result != 0)
		return result;
	result = cluster_wal_origin_inputs_read_locked(&work->base, node, &work->retained);
	if (result != 0)
		return result;
	if (history->count + history->terminal_count >= CLUSTER_WAL_HISTORY_MAX_RECORDS)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	if (work->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	terminal->closure_version = 1;
	terminal->generation = work->base.header.file_txn_seq + 1;
	terminal->database_incarnation = work->base.header.v2.database_incarnation;
	terminal->sealing_sequence = work->base.header.file_txn_seq;
	memcpy(terminal->sealing_sha256, work->serial->pending.file.image_sha256, 32);
	memcpy(terminal->operation_uuid, work->input.startup.operation_uuid, 16);
	terminal->recoverer_node = work->publisher_node;
	terminal->recoverer_incarnation = work->publisher_incarnation;
	terminal->observation = work->input.observation;
	result = cluster_control_root_v3_startup_encode(&work->base, node, &work->input.startup,
													terminal->original, &terminal->original_ref);
	if (result != 0)
		return result;
	if (memcmp(&terminal->original_ref, &work->base.startup[node], sizeof(terminal->original_ref))
		!= 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = initializer_terminal_digest(work, &ref);
	if (result != 0)
		return result;
	if (!initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result
		= cluster_wal_terminal_prepare(&work->base, node, history, terminal, &work->terminal_stage);
	if (result == 0)
		result = cluster_wal_terminal_install(&work->terminal_stage);
	if (result != 0)
		return result;
	while (position < history->terminal_count
		   && history->terminals[position].incarnation
				  < work->serial->duty.origin_owner_incarnation)
		position++;
	if (position < history->terminal_count
		&& history->terminals[position].incarnation == work->serial->duty.origin_owner_incarnation)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	memmove(&history->terminals[position + 1], &history->terminals[position],
			(history->terminal_count - position) * sizeof(history->terminals[0]));
	history->terminals[position].incarnation = work->serial->duty.origin_owner_incarnation;
	history->terminals[position].generation = work->terminal_stage.generation;
	memcpy(history->terminals[position].sha256, work->terminal_stage.sha256, 32);
	history->terminal_count++;
	result = cluster_wal_history_prepare(&work->base, node, history, terminal->generation,
										 terminal->operation_uuid, &work->history_stage);
	if (result == 0)
		result = cluster_wal_history_install(&work->history_stage);
	if (result != 0)
		return result;
	work->cf_held = false;
	result = release_cf(ExclusiveLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != 0)
		return result;
	if (!initializer_terminal_current(work)
		|| cluster_wal_retention_pin_seal_for_root_publish(work->pin) != CLUSTER_WAL_PIN_OK)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_recovery_serial_release(work->serial) != CLUSTER_RECOVERY_SERIAL_RELEASE_CONFIRMED)
		return CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	if (cluster_wal_retention_pending_publish_begin(&work->serial->duty, &work->serial->pending,
													&work->walr)
		!= CLUSTER_WAL_PIN_OK)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = initializer_terminal_read(work, &work->observed);
	if (result != 0)
		return result;
	if (memcmp(work->base.bytes, work->observed.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	work->next = work->base;
	work->next.refs[node].history_generation = work->history_stage.generation;
	memcpy(work->next.refs[node].history_sha256, work->history_stage.sha256, 32);
	memset(&work->next.startup[node], 0, sizeof(work->next.startup[node]));
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = GetCurrentTimestamp();
	result = cluster_control_root_v3_encode(&work->next);
	if (result != 0)
		return result;
	if (!initializer_terminal_current(work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_control_version(work->serial->duty.storage_uuid,
								  work->serial->duty.system_identifier, &work->observed, &control,
								  &file, 3);
	if (result != 0)
		return result;
	if (memcmp(work->observed.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	result = cluster_wal_terminal_read_locked(&work->observed, node, position, &work->readback);
	if (result != 0)
		return result;
	return initializer_terminal_current(work) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
											  : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
}

static ClusterControlRootResult
initializer_terminal_cleanup(InitializerTerminalWork *work, ClusterControlRootResult result)
{
	/* Never remove a formal object, including an unselected/uncertain one. */
	if (work->anchor_stage.state != 0
		&& cluster_recovery_anchor_v2_discard(&work->anchor_stage) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (work->terminal_stage.state != 0 && cluster_wal_terminal_discard(&work->terminal_stage) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (work->history_stage.state != 0 && cluster_wal_history_discard(&work->history_stage) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (work->cf_held) {
		work->cf_held = false;
		result = release_cf(ExclusiveLock, result);
	}
	if (work->walr != NULL
		&& cluster_wal_retention_root_publish_end(&work->walr) != CLUSTER_WALR_RELEASE_CONFIRMED)
		result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	return result;
}

ClusterControlRootResult
cluster_control_root_v3_initializer_finish(ClusterRecoverySerialGuard *serial,
										   ClusterWalRetentionPin *pin)
{
	InitializerTerminalWork *work;
	ClusterControlRootResult result;
	if (!cluster_shared_config || !cluster_enabled || !cluster_controlfile_shared_authority
		|| !enableFsync || serial == NULL || pin == NULL || CritSectionCount != 0
		|| cluster_node_id < 0 || cluster_node_id >= CLUSTER_MAX_NODES || cluster_cf_held(ShareLock)
		|| cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->serial = serial;
	work->pin = pin;
	work->publisher_node = cluster_node_id;
	work->publisher_incarnation = cluster_qvotec_get_self_incarnation();
	PG_TRY();
	{
		result = initializer_terminal_execute(work);
	}
	PG_CATCH();
	{
		result = initializer_terminal_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		if (result == CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN)
			elog(FATAL, "could not confirm initializer terminal publisher cleanup");
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = initializer_terminal_cleanup(work, result);
	pfree(work);
	return result;
}

static bool
failure_v2_current(const ClusterRecoverySerialRequest *request, FailureInputV2Work *work)
{
	return failure_v2_authorized(request, work)
		   && (work->self
			   || cluster_recovery_serial_input_revalidate(&work->serial)
					  == CLUSTER_RECOVERY_SERIAL_CURRENT);
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
	result = read_thread_version(&request->duty, &work->base, &work->control, &file_token,
								 work->format_version);
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
	ClusterWalTailObservation fresh;
	ClusterControlRootSnapshot *record;
	int index = request->duty.origin_thread_id - 1;

	result = failure_v2_tail_read(request, work, ExclusiveLock);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	record = &work->base.records[index];
	result = cluster_wal_tail_observe_checkpoint(
		cluster_wal_threads_dir, &work->source_ref, wal_segment_size, record->checkpoint_lower_lsn,
		work->tail.complete_end, work->control.checkPoint, record->checkpoint_record_crc32c, &fresh);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(&fresh, &work->tail, sizeof(fresh)) != 0)
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
	result = encode_extended_image(&work->next, work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!failure_v2_current(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_thread_version(&request->duty, &work->base, &work->control, &file_token,
								 work->format_version);
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
	/* PGRAC: a self-seal holds no IR; the root CAS and WALR still apply. */
	if (!work->self) {
		acquired = cluster_recovery_serial_acquire(request, &work->serial);
		if (acquired != CLUSTER_RECOVERY_SERIAL_GRANTED)
			return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	}
	result = failure_v2_tail_read(request, work, ShareLock);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	record = &work->base.records[index];
	if (work->control.checkPoint < record->checkpoint_lower_lsn
		|| work->control.checkPoint == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_RANGE_INVALID;
	work->source_ref.claim.identity = record->identity;
	work->source_ref.claim.database_incarnation = work->base.header.v2.database_incarnation;
	work->source_ref.claim.max_config_generation = work->base.header.v2.config_generation;
	memcpy(work->source_ref.claim.claim_sha256, work->base.refs[index].claim_sha256, 32);
	work->source_ref.timeline = record->checkpoint_tli;
	work->cf_mode = NoLock;
	result = release_cf(ShareLock, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	result = cluster_wal_tail_observe_checkpoint(
		cluster_wal_threads_dir, &work->source_ref, wal_segment_size, record->checkpoint_lower_lsn,
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
	result = read_thread_version(&request->duty, &work->base, &work->control, &file_token,
								 work->format_version);
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
	result = encode_extended_image(&work->next, work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!failure_v2_authorized(request, work))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_thread_version(&request->duty, &work->base, &work->control, &file_token,
								 work->format_version);
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
				   ClusterControlRootReadToken *out_token, bool seal_tail, uint16 version,
				   const ClusterWalSourceRef *self_restart, uint64 self_min_dead_us)
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
		|| request->duty.system_identifier != GetSystemIdentifier()
		|| (self_restart == NULL
			&& (request->formation == NULL || request->fence_need_set == NULL
				|| request->fence_admission_set == NULL)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work = palloc0(sizeof(*work));
	work->format_version = version;
	work->publisher_node = cluster_node_id;
	work->publisher_incarnation = cluster_qvotec_get_self_incarnation();
	if (self_restart != NULL) {
		work->self = true;
		work->self_min_dead_us = self_min_dead_us;
		work->self_restart = *self_restart;
	}
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
	return failure_v2_publish(request, out, out_token, false, 2, NULL, 0);
}

ClusterControlRootResult
cluster_control_root_v2_failure_tail_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	return failure_v2_publish(request, out, out_token, true, 2, NULL, 0);
}

ClusterControlRootResult
cluster_control_root_v3_failure_open_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	return failure_v2_publish(request, out, out_token, false, 3, NULL, 0);
}

ClusterControlRootResult
cluster_control_root_v3_failure_tail_publish(const ClusterRecoverySerialRequest *request,
											 ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	return failure_v2_publish(request, out, out_token, true, 3, NULL, 0);
}

/*
 * PGRAC (S9P2-05, PU-D-5): seal this node's own crashed generation -- OPEN
 * to RECOVERY_REQUIRED, then the validated tail -- with the self-seal
 * evidence instead of an external fence (self_predecessor_authorized).  A
 * generation already sealed with its tail is reported as it is.  Each step
 * is a compare-and-swap on the root just read.  Author: SqlRush
 * <sqlrush@gmail.com>
 */
ClusterControlRootResult
cluster_control_root_v3_self_seal_v1(const ClusterWalSourceRef *restart, uint64 min_dead_us,
									 ClusterControlRootSnapshot *out,
									 ClusterControlRootReadToken *out_token)
{
	const uint32 required
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	ClusterRecoverySerialRequest request;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootResult result;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (restart == NULL || out == NULL || out_token == NULL || cluster_node_id < 0
		|| restart->claim.identity.origin_node_id != cluster_node_id
		|| restart->claim.identity.origin_thread_id != (uint16)(cluster_node_id + 1))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_control_root_read_canonical(
		restart->claim.identity.origin_thread_id, &restart->claim.identity,
		CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot, &token);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	memset(&request, 0, sizeof(request));
	request.mode = CLUSTER_RECOVERY_SERIAL_INPUT_SEAL;
	request.duty = restart->claim.identity;
	request.expected_root_token = token;
	if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN) {
		result = failure_v2_publish(&request, &snapshot, &token, false, 3, restart, min_dead_us);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		request.expected_root_token = token;
	}
	if (snapshot.lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		&& (snapshot.root_flags & CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID) == 0) {
		result = failure_v2_publish(&request, &snapshot, &token, true, 3, restart, min_dead_us);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (snapshot.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (snapshot.root_flags & required) != required)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	*out = snapshot;
	*out_token = token;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: the raw shutdown anchor and native insertion cut are independent
 * facts. Generic root views deliberately still report IN_PRODUCTION here.
 * Author: SqlRush <sqlrush@gmail.com> */
static bool
shutdown_v2_owner_current(const ClusterWalSourceRef *expected, uint64 epoch, XLogRecPtr end)
{
	return ShutdownRequestPending
		   && checkpoint_v2_owner_current(&expected->claim.identity, epoch, expected->timeline, end)
		   /* The next record START skips a page header at an exact boundary;
			* only the reserved END is comparable with this record's end. */
		   && GetXLogInsertEndRecPtr() == end;
}

static ClusterControlRootResult
shutdown_v2_observe_work(CheckpointV2Work *work, const ClusterWalSourceRef *expected,
						 uint64 epoch, const ClusterPhase1FullStopPlan *close_plan)
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
	result = read_thread_version(self, &work->base, &work->new_view, &work->before,
								 work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	record = &work->base.records[index];
	if (work->base.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| (work->base.header.v2.database_state != CLUSTER_CONTROL_ROOT_DATABASE_OPEN
			&& !(close_plan != NULL
				 && work->base.header.v2.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED))
		|| (record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN
			&& !(close_plan != NULL && record->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED))
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
		|| record->checkpoint_lower_lsn == InvalidXLogRecPtr
		|| record->checkpoint_lower_lsn > work->old_view.checkPoint
		|| (work->format_version < 3 && record->checkpoint_lower_lsn != work->old_view.checkPoint)
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
	if (work->format_version >= 3)
		work->retained_lower = record->checkpoint_lower_lsn;
	if (!shutdown_v2_owner_current(expected, epoch, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (close_plan != NULL && record->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED) {
		ClusterControlRootStopPhase phase;
		/* Already-published closure is immutable evidence, not an active
		 * generation retention grant. Do not reopen or reacquire writer WALR. */
		result = stop_phase_v2_locked(&work->base, index, &work->new_view,
									  self->origin_owner_incarnation, &phase);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
		work->next = work->base;
		work->after = work->before;
		return phase == CLUSTER_CONTROL_ROOT_STOP_CLOSED ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
														 : CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	}
	work->source_ref = *expected;
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
	result = checkpoint_v2_input_observe(work, &work->old_view, end, work->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(close_plan != NULL ? ExclusiveLock : ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = close_plan != NULL ? ExclusiveLock : ShareLock;
	result = read_thread_version(self, &work->next, &work->new_view, &work->after,
								 work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (!file_token_equal(&work->before, &work->after))
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	result = checkpoint_v2_input_observe(work, &work->old_view, end, work->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!checkpoint_v2_wal_paths_current(work, self)
		|| !shutdown_v2_owner_current(expected, epoch, end))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
shutdown_observe_version(const ClusterWalSourceRef *expected,
						 ClusterControlRootSnapshot *out, ClusterControlRootFileToken *out_token,
						 uint16 version)
{
	CheckpointV2Work *work;
	ClusterWalSourceRef ref;
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
	work->format_version = version;
	for (size_t i = 0; i < lengthof(work->wal_dirs); ++i)
		work->wal_dirs[i] = -1;
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i)
		work->wal_segments[i] = -1;
	PG_TRY();
	{
		result = shutdown_v2_observe_work(work, &ref, epoch, NULL);
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

/* Full stop retains its declared control participants until the final
 * receipt exchange. It never drops an inconvenient or already-closed peer. */
static ClusterControlRootResult
normal_stop_v2_publish_work(CheckpointV2Work *work, const ClusterWalSourceRef *ref,
							const ClusterPhase1FullStopPlan *plan, uint64 members, bool *complete)
{
	const ClusterControlRootIdentity *self = &ref->claim.identity;
	ClusterControlRootSnapshot *own = &work->next.records[self->origin_thread_id - 1];
	ClusterControlRootResult result;
	ControlFileData closed_view = work->old_view;
	ClusterRecoveryAnchorRefV2 anchor = { 0 };
	bool already_closed = own->lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	int index = self->origin_thread_id - 1;

	if (work->next.header.v2.serving[0] != members || work->next.header.v2.serving[1] != 0
		|| !cluster_normal_stop_durable_close_owned(plan))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	for (int node = 0; node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; node++)
		if (plan->member_incarnations[node] != 0
			&& (!work->next.present[node]
				|| work->next.records[node].identity.origin_owner_incarnation
					   != plan->member_incarnations[node]))
			return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	if (!already_closed && (work->cf_mode != ExclusiveLock || work->walr == NULL))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	anchor.identity = *self;
	anchor.database_incarnation = work->next.header.v2.database_incarnation;
	anchor.max_config_generation = work->next.header.v2.config_generation;
	anchor.anchor_generation = work->next.refs[index].anchor_generation;
	memcpy(anchor.anchor_sha256, work->next.refs[index].anchor_sha256, 32);
	memcpy(anchor.claim_sha256, work->next.refs[index].claim_sha256, 32);
	own->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	result = cluster_recovery_anchor_v2_thread_state(&anchor, own, &closed_view);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	*complete = true;
	work->close_history = palloc(sizeof(*work->close_history));
	for (uint32 node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		const ClusterControlRootSnapshot *record = &work->next.records[node];
		bool configured
			= (work->next.header.v2.configured[node / 64] & (UINT64_C(1) << (node % 64))) != 0;
		if (!work->next.present[node]) {
			if (configured)
				return CLUSTER_CONTROL_ROOT_ABSENT;
			continue;
		}
		/* A closed predecessor does not close its selected successor's
		 * initialization. Preserve this reference and leave global close to
		 * the operation owner; a checkpointer cannot cancel/adopt that work. */
		if (work->next.startup[node].generation != 0)
			*complete = false;
		if (work->next.refs[node].history_generation != 0) {
			result = cluster_wal_history_read_locked(&work->next, node, work->close_history);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				return result;
			for (uint32 i = 0; i < work->close_history->count; i++) {
				const ClusterWalHistoryRecord *old = &work->close_history->records[i];
				result = closed_v2_record_locked(&work->next, &old->snapshot, &old->refs,
												 &work->new_view);
				if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
					return result;
			}
		}
		if (record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED) {
			*complete = false;
			continue;
		}
		if (node == (uint32)index)
			continue; /* Exact shutdown WAL verified above. */
		result
			= closed_v2_record_locked(&work->next, record, &work->next.refs[node], &work->new_view);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	if (work->next.header.v2.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED && !*complete)
		return CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
	if (already_closed) {
		/* A peer still owns the last publication. Do not reacquire active
		 * writer permission or rewrite a previous immutable close. */
		*complete = *complete
					&& work->next.header.v2.database_state == CLUSTER_CONTROL_ROOT_DATABASE_CLOSED;
		return cluster_normal_stop_durable_close_owned(plan) ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
															 : CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	}
	if (own->root_publish_seq == UINT64_MAX || work->next.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	own->root_publish_seq++;
	own->published_at_usec = GetCurrentTimestamp();
	own->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_CLEAN_CLOSE;
	work->next.publisher_incarnation[index] = self->origin_owner_incarnation;
	work->next.publisher_node[index] = self->origin_node_id;
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = own->published_at_usec;
	if (*complete)
		work->next.header.v2.database_state = CLUSTER_CONTROL_ROOT_DATABASE_CLOSED;
	result = encode_extended_image(&work->next, work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!cluster_normal_stop_durable_close_owned(plan)
		|| !checkpoint_v2_wal_paths_current(work, self)
		|| !shutdown_v2_owner_current(ref, plan->epoch, own->validated_tail_lsn_exclusive))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = checkpoint_v2_input_observe(work, &work->old_view, own->validated_tail_lsn_exclusive,
										  work->checkpoint_crc);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_thread_version(self, &work->base, &work->new_view, &work->after,
								 work->format_version);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& result != CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED)
		return result;
	if (memcmp(work->base.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	if (!cluster_normal_stop_durable_close_owned(plan)
		|| !shutdown_v2_owner_current(ref, plan->epoch, own->validated_tail_lsn_exclusive))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	return cluster_cf_control_projection_write_locked(&work->new_view);
}

static ClusterControlRootResult
normal_stop_close_version(const ClusterPhase1FullStopPlan *plan, bool *all_closed, uint16 version)
{
	CheckpointV2Work *work;
	ClusterWalSourceRef ref;
	ClusterControlRootResult result;
	uint64 members = 0;
	bool complete = false;
	int index;

	if (all_closed != NULL)
		*all_closed = false;
	if (all_closed == NULL || plan == NULL || !plan->valid || !plan->pre2_root_observed
		|| !cluster_shared_config || !AmCheckpointerProcess() || !ShutdownRequestPending
		|| !enableFsync || !cluster_normal_stop_durable_close_owned(plan)
		|| !cluster_wal_thread_current_v2_ref(&ref) || ref.claim.identity.origin_thread_id == 0
		|| ref.claim.identity.origin_thread_id > CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| ref.claim.identity.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	index = ref.claim.identity.origin_thread_id - 1;
	if (index >= CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT || ref.claim.identity.origin_node_id != index
		|| plan->member_incarnations[index] != ref.claim.identity.origin_owner_incarnation
		|| plan->own_wal_started_at != ref.claim.identity.thread_claim_created_at
		|| !checkpoint_v2_owner_current(&ref.claim.identity, plan->epoch, ref.timeline, 0))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	for (int node = 0; node < CLUSTER_PHASE1_FULL_STOP_MEMBER_COUNT; node++)
		if (plan->member_incarnations[node] != 0)
			members |= UINT64_C(1) << node;
	work = palloc0(sizeof(*work));
	work->purpose = CHECKPOINT_V2_SHUTDOWN_EVIDENCE;
	work->format_version = version;
	for (size_t i = 0; i < lengthof(work->wal_dirs); ++i)
		work->wal_dirs[i] = -1;
	for (size_t i = 0; i < lengthof(work->wal_segments); ++i)
		work->wal_segments[i] = -1;
	PG_TRY();
	{
		result = shutdown_v2_observe_work(work, &ref, plan->epoch, plan);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = normal_stop_v2_publish_work(work, &ref, plan, members, &complete);
	}
	PG_CATCH();
	{
		(void)checkpoint_v2_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = checkpoint_v2_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY
		&& (!cluster_normal_stop_durable_close_owned(plan)
			|| !shutdown_v2_owner_current(&ref, plan->epoch,
										  work->base.records[index].validated_tail_lsn_exclusive)))
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*all_closed = complete;
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_control_root_v2_shutdown_observe(const ClusterWalSourceRef *ref,
										 ClusterControlRootSnapshot *out,
										 ClusterControlRootFileToken *token)
{
	return shutdown_observe_version(ref, out, token, 2);
}

ClusterControlRootResult
cluster_control_root_v2_normal_stop_close(const ClusterPhase1FullStopPlan *plan, bool *all_closed)
{
	return normal_stop_close_version(plan, all_closed, 2);
}

/* Explicit new-format publishers share physical WAL/claim/anchor checks,
 * never old-format fallback or loss of another origin's pending reference. */
ClusterControlRootResult
cluster_control_root_v3_checkpoint_publish(const ClusterControlRootIdentity *self,
										   const ControlFileData *control, XLogRecPtr end,
										   ClusterControlRootSnapshot *out,
										   ClusterControlRootFileToken *token,
										   ControlFileData *view)
{
	return checkpoint_v2_publish(CHECKPOINT_V2_ONLINE, self, control, end, out, token, view, 3);
}

ClusterControlRootResult
cluster_control_root_v3_shutdown_checkpoint_publish(const ClusterControlRootIdentity *self,
													const ControlFileData *control, XLogRecPtr end,
													ClusterControlRootSnapshot *out,
													ClusterControlRootFileToken *token,
													ControlFileData *view)
{
	return checkpoint_v2_publish(CHECKPOINT_V2_SHUTDOWN_EVIDENCE, self, control, end, out, token,
								 view, 3);
}

ClusterControlRootResult
cluster_control_root_v3_shutdown_observe(const ClusterWalSourceRef *ref,
										 ClusterControlRootSnapshot *out,
										 ClusterControlRootFileToken *token)
{
	return shutdown_observe_version(ref, out, token, 3);
}

ClusterControlRootResult
cluster_control_root_v3_normal_stop_close(const ClusterPhase1FullStopPlan *plan, bool *all_closed)
{
	return normal_stop_close_version(plan, all_closed, 3);
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

/* PGRAC: only the existing post-IR terminal owner uses this v3 adapter.
 * It preserves immutable input references and never installs a pending writer
 * or grants serving. Author: SqlRush <sqlrush@gmail.com> */
typedef struct RecoveryCompleteV3Work {
	ControlRootImage base, next, observed;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterWalRootPublishGuard *walr;
	bool cf_held;
	int32 publisher_node;
	uint64 publisher_incarnation;
} RecoveryCompleteV3Work;

static ClusterControlRootResult
recovery_complete_v3_cleanup(RecoveryCompleteV3Work *work, ClusterControlRootResult result)
{
	if (work->cf_held) {
		work->cf_held = false;
		result = release_cf(ExclusiveLock, result);
	}
	if (work->walr != NULL
		&& cluster_wal_retention_root_publish_end(&work->walr) != CLUSTER_WALR_RELEASE_CONFIRMED)
		result = CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN;
	return result;
}

static bool
recovery_complete_v3_owner_current(const RecoveryCompleteV3Work *work,
								   const ClusterControlRootReadToken *expected)
{
	return cluster_node_id == work->publisher_node
		   && cluster_qvotec_get_self_incarnation() == work->publisher_incarnation
		   && work->publisher_incarnation != 0
		   && cluster_wal_retention_root_publish_sealed_current(work->walr, expected);
}

static ClusterControlRootResult
recovery_complete_v3_work(RecoveryCompleteV3Work *work, const ClusterControlRootReadToken *expected,
						  const ClusterControlRootPatch *patch)
{
	const uint32 required
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
	const uint32 recovered = CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID
							 | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_LAST_RECORD_VALID;
	ClusterControlRootResult result;
	ClusterWalPinResult pinned;
	ClusterControlRootFileToken file_token;
	ClusterControlRootReadToken actual;
	ClusterControlRootIdentity identity;
	ClusterWalSourceRef source_ref = { 0 };
	ClusterWalTailObservation tail;
	ControlFileData control;
	ClusterControlRootSnapshot *record;
	uint8 storage_uuid[16];
	uint32 node = expected->origin_thread_id - 1;

	/* Keep this recovery's sealed S through short reader conflicts. Each
	 * attempt rechecks the original owner and confirmed IR release; no CF
	 * lock, DATA work or converted guard may span the backoff. Exhaustion
	 * leaves the original worker responsible for deferral and cleanup. */
	for (int attempt = 0; attempt < 8; attempt++) {
		pinned = cluster_wal_retention_root_publish_begin_exact(expected, true, &work->walr);
		if (pinned != CLUSTER_WAL_PIN_UNAVAILABLE || work->walr != NULL || attempt == 7)
			break;
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		(void)WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						1L << Min(attempt, 5), WAIT_EVENT_CLUSTER_THREAD_RECOVERY);
		CHECK_FOR_INTERRUPTS();
	}
	if (pinned != CLUSTER_WAL_PIN_OK)
		return pinned == CLUSTER_WAL_PIN_INVALID ? CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT
			   : pinned == CLUSTER_WAL_PIN_STALE ? CLUSTER_CONTROL_ROOT_STALE_TOKEN
												 : CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	if (!recovery_complete_v3_owner_current(work, expected))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	result = storage_contract_check(NULL, true);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!acquire_clusterwide_cf(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_held = true;
	if (!current_storage_uuid(storage_uuid))
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	result = read_control_version(storage_uuid, GetSystemIdentifier(), &work->base, &control,
								  &file_token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!work->base.present[node])
		return CLUSTER_CONTROL_ROOT_ABSENT;
	make_read_token(&work->base, expected->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY, &actual);
	if (!read_token_equal(expected, &actual))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	record = &work->base.records[node];
	if (work->base.header.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE
		|| work->base.header.v2.database_state < CLUSTER_CONTROL_ROOT_DATABASE_MOUNTED
		|| work->base.header.v2.database_state > CLUSTER_CONTROL_ROOT_DATABASE_OPEN
		|| work->base.startup[node].generation != 0
		|| (work->base.header.v2.serving[node / 64] & (UINT64_C(1) << (node % 64))) != 0
		|| record->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| (record->root_flags & required) != required
		|| (record->root_flags & patch->expected_flags_mask) != patch->expected_flags_value
		|| patch->desired.root_flags != (record->root_flags | recovered)
		|| patch->desired.recovered_tli != record->tail_tli
		|| patch->desired.recovered_through_lsn_exclusive != record->validated_tail_lsn_exclusive
		|| patch->desired.recovered_last_record_lsn != record->tail_last_record_lsn
		|| patch->desired.recovered_last_record_crc32c != record->tail_last_record_crc32c)
		return CLUSTER_CONTROL_ROOT_CAS_CONFLICT;
	if (record->root_publish_seq == UINT64_MAX || work->base.header.file_txn_seq == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	identity = record->identity;
	if (identity.origin_node_id == work->publisher_node
		&& identity.origin_owner_incarnation == work->publisher_incarnation)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	/* Authenticate physical inputs without changing the selected checkpoint. */
	result = read_thread_version(&identity, &work->observed, &control, &file_token, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(work->observed.bytes, work->base.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	source_ref.claim.identity = identity;
	source_ref.claim.database_incarnation = work->base.header.v2.database_incarnation;
	source_ref.claim.max_config_generation = work->base.header.v2.config_generation;
	memcpy(source_ref.claim.claim_sha256, work->base.refs[node].claim_sha256, 32);
	source_ref.timeline = record->tail_tli;
	result = cluster_wal_tail_visit_sealed(cluster_wal_threads_dir, &source_ref,
										 wal_segment_size, record, control.checkPoint,
										 NULL, NULL, &tail);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work->next = work->base;
	record = &work->next.records[node];
	if (!apply_patch(record, patch))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	record->root_publish_seq++;
	record->published_at_usec = GetCurrentTimestamp();
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE;
	work->next.publisher_node[node] = work->publisher_node;
	work->next.publisher_incarnation[node] = work->publisher_incarnation;
	work->next.header.file_txn_seq++;
	work->next.header.published_at_usec = record->published_at_usec;
	result = cluster_control_root_v3_encode(&work->next);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!recovery_complete_v3_owner_current(work, expected))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (!publish_updated_image(&work->base, &work->next))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	result = read_thread_version(&identity, &work->observed, &control, &file_token, 3);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (memcmp(work->observed.bytes, work->next.bytes, CLUSTER_CONTROL_ROOT_FILE_BYTES) != 0)
		return CLUSTER_CONTROL_ROOT_POSTREAD_FAILED;
	if (!recovery_complete_v3_owner_current(work, expected))
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	work->snapshot = work->observed.records[node];
	make_read_token(&work->observed, expected->origin_thread_id, CONTROL_ROOT_SOURCE_PRIMARY,
					&work->token);
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static ClusterControlRootResult
recovery_complete_v3_publish(const ClusterControlRootReadToken *expected,
							 const ClusterControlRootPatch *patch, ClusterControlRootSnapshot *out,
							 ClusterControlRootReadToken *out_token)
{
	RecoveryCompleteV3Work *work;
	ClusterControlRootResult result;
	if (cluster_node_id < 0 || cluster_node_id >= CLUSTER_CONTROL_ROOT_RECORD_COUNT
		|| CritSectionCount != 0 || !cluster_enabled || !cluster_controlfile_shared_authority
		|| patch->expected_lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED
		|| patch->desired.lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE
		|| cluster_cf_held_is_clusterwide(ShareLock)
		|| cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	work = palloc0(sizeof(*work));
	work->publisher_node = cluster_node_id;
	work->publisher_incarnation = cluster_qvotec_get_self_incarnation();
	PG_TRY();
	{
		result = recovery_complete_v3_work(work, expected, patch);
	}
	PG_CATCH();
	{
		(void)recovery_complete_v3_cleanup(work, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	result = recovery_complete_v3_cleanup(work, result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		if (out != NULL)
			*out = work->snapshot;
		if (out_token != NULL)
			*out_token = work->token;
	}
	pfree(work);
	return result;
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
	bool alias
		= cluster_shared_config && reason == CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE
		  && (history_ranges_overlap(expected_token, sizeof(*expected_token), out_snapshot,
									 sizeof(*out_snapshot))
			  || history_ranges_overlap(expected_token, sizeof(*expected_token), out_token,
										sizeof(*out_token))
			  || history_ranges_overlap(patch, sizeof(*patch), out_snapshot, sizeof(*out_snapshot))
			  || history_ranges_overlap(patch, sizeof(*patch), out_token, sizeof(*out_token))
			  || history_ranges_overlap(out_snapshot, sizeof(*out_snapshot), out_token,
										sizeof(*out_token)));

	if (out_snapshot != NULL)
		memset(out_snapshot, 0, sizeof(*out_snapshot));
	if (out_token != NULL)
		memset(out_token, 0, sizeof(*out_token));
	if (alias || expected_token == NULL || expected_token->source != CONTROL_ROOT_SOURCE_PRIMARY
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
	if (cluster_shared_config && reason == CLUSTER_CONTROL_ROOT_PUBLISH_RECOVERY_COMPLETE)
		return recovery_complete_v3_publish(expected_token, patch, out_snapshot, out_token);
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

/* PGRAC: keep the exact shared recovery source inside one owned read scope.
 * No raw reader/FD escapes to a caller and no provisional record is authority.
 * Author: SqlRush <sqlrush@gmail.com> */
typedef struct RecoveryWalVisitWork {
	ControlRootImage image;
	ClusterControlRootSnapshot expected;
	ClusterControlRootReadToken token;
	ClusterWalSourceRef ref;
	ControlFileData control;
	ClusterWalTailObservation tail;
	LOCKMODE cf_mode;
} RecoveryWalVisitWork;

static ClusterControlRootResult
recovery_wal_visit_root(RecoveryWalVisitWork *work)
{
	ClusterControlRootFileToken file_token;
	ClusterControlRootReadToken actual;
	ClusterControlRootResult result;
	unsigned node = work->expected.identity.origin_node_id;
	if (!acquire_clusterwide_cf(ShareLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	work->cf_mode = ShareLock;
	result = read_thread_version(&work->expected.identity, &work->image, &work->control,
								 &file_token, CONTROL_ROOT_HEADER_VERSION_V3);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		make_read_token(&work->image, node + 1, CONTROL_ROOT_SOURCE_PRIMARY, &actual);
		if (!read_token_equal(&work->token, &actual)
			|| memcmp(&work->expected, &work->image.records[node], sizeof(work->expected)) != 0
			|| work->image.startup[node].generation != 0)
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		else {
			work->ref.claim.identity = work->expected.identity;
			work->ref.claim.database_incarnation = work->image.header.v2.database_incarnation;
			work->ref.claim.max_config_generation = work->image.header.v2.config_generation;
			memcpy(work->ref.claim.claim_sha256, work->image.refs[node].claim_sha256, 32);
			work->ref.timeline = work->expected.checkpoint_tli;
			if (!cluster_wal_claim_v2_ref_valid(&work->ref.claim) || work->ref.timeline == 0)
				result = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		}
	}
	work->cf_mode = NoLock;
	return release_cf(ShareLock, result);
}

ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *expected,
										const ClusterControlRootReadToken *token,
										ClusterWalSourceRef *out, XLogRecPtr *native_redo)
{
	RecoveryWalVisitWork *work;
	ClusterControlRootResult result;

	if (history_ranges_overlap(expected, sizeof(*expected), out, sizeof(*out))
		|| history_ranges_overlap(token, sizeof(*token), out, sizeof(*out))
		|| history_ranges_overlap(expected, sizeof(*expected), native_redo, sizeof(*native_redo))
		|| history_ranges_overlap(token, sizeof(*token), native_redo, sizeof(*native_redo))
		|| history_ranges_overlap(out, sizeof(*out), native_redo, sizeof(*native_redo)))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (native_redo != NULL)
		*native_redo = InvalidXLogRecPtr;
	if (expected == NULL || token == NULL || out == NULL || native_redo == NULL
		|| expected->identity.origin_node_id < 0
		|| expected->identity.origin_node_id >= CLUSTER_MAX_NODES
		|| expected->identity.origin_thread_id != expected->identity.origin_node_id + 1
		|| expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_enabled || !cluster_shared_config || !cluster_controlfile_shared_authority
		|| CritSectionCount != 0 || expected->identity.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = storage_contract_check(expected->identity.storage_uuid, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	work->expected = *expected;
	work->token = *token;
	PG_TRY();
	{
		result = recovery_wal_visit_root(work);
	}
	PG_CATCH();
	{
		if (work->cf_mode != NoLock)
			(void)release_cf(work->cf_mode, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		*out = work->ref;
		*native_redo = work->control.checkPointCopy.redo;
	}
	pfree(work);
	return result;
}

ClusterControlRootResult
cluster_control_root_recovery_visit(const ClusterControlRootSnapshot *expected,
									const ClusterControlRootReadToken *token,
									ClusterWalRecordVisitor visitor, void *arg,
									ClusterWalTailObservation *out)
{
	RecoveryWalVisitWork *work;
	ClusterControlRootResult result;
	bool alias = history_ranges_overlap(expected, sizeof(*expected), out, sizeof(*out))
				 || history_ranges_overlap(token, sizeof(*token), out, sizeof(*out));
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (alias || expected == NULL || token == NULL || out == NULL
		|| expected->identity.origin_node_id < 0
		|| expected->identity.origin_node_id >= CLUSTER_MAX_NODES
		|| expected->identity.origin_thread_id != expected->identity.origin_node_id + 1
		|| expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_enabled || !cluster_shared_config || !cluster_controlfile_shared_authority
		|| CritSectionCount != 0 || expected->identity.system_identifier != GetSystemIdentifier())
		return CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	if (cluster_cf_held(ShareLock) || cluster_cf_held(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	result = storage_contract_check(expected->identity.storage_uuid, false);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	work = palloc0(sizeof(*work));
	work->expected = *expected;
	work->token = *token;
	PG_TRY();
	{
		result = recovery_wal_visit_root(work);
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
			result = cluster_wal_tail_visit_sealed(
				cluster_wal_threads_dir, &work->ref, wal_segment_size, &work->expected,
				work->control.checkPoint, visitor, arg, &work->tail);
		}
		if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			result = recovery_wal_visit_root(work);
	}
	PG_CATCH();
	{
		if (work->cf_mode != NoLock)
			(void)release_cf(work->cf_mode, CLUSTER_CONTROL_ROOT_IO_ERROR);
		pfree(work);
		PG_RE_THROW();
	}
	PG_END_TRY();
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = work->tail;
	pfree(work);
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
