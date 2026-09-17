/*-------------------------------------------------------------------------
 *
 * test_cluster_control_root.c
 *	  RF-ROOT P1 tests for the survivor-readable control-root carrier.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlog.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#include "../../backend/cluster/cluster_control_root_private.h"

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"

/* backend global provided by xlog.c in a real server; the cluster_unit
 * fixture uses the default 16MiB segment size (segment 1 covers
 * [0x1000000, 0x2000000) — the build_source_wal_state fixture's
 * checkpoint LSN 0x1000000 therefore lives in segment 1). */
int wal_segment_size = XLOG_BLCKSZ * 2048;


UT_DEFINE_GLOBALS();

#define TEST_SYSID UINT64_C(0x0123456789abcdef)

char *cluster_shared_data_dir = NULL;
char *cluster_wal_threads_dir = NULL;
char *DataDir = NULL;
int cluster_node_id = 0;

static char test_root[MAXPGPATH];
static char test_wal_root[MAXPGPATH];
static uint64 test_system_identifier = TEST_SYSID;
static char test_storage_uuid_text[33] = "00112233445566778899aabbccddeeff";
static ClusterCfContractState test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
static int test_node_count = 4;
static bool test_local_probe = true;
static bool test_cf_grant = true;
static bool test_cf_clusterwide = true;
static bool test_cf_release_confirmed = true;
static int test_cf_lock_calls = 0;
static int test_durable_rename_calls = 0;
static bool test_fail_primary_rename = false;
static bool test_create_authorized = true;
static uint16 test_own_thread = 1;
/* RF-ROOT P9 verification (contract): stub state for the bit22 latch
 * cross-restart restore (cluster_control_root_restore_bit22_latch_if_active
 * links the semantic_activation entry points; the unit harness stands in
 * for the shmem latch with plain scalars). */
static bool test_bit22_latch_active;
static bool test_bit22_latch_apply_ok = true;
static uint64 test_bit22_latch_apply_epoch;
static uint64 test_bit22_latch_apply_generation;
static int test_bit22_latch_apply_calls;
/* RF-ROOT P9 verification: durable-OPEN restore stub state — the harness
 * stands in for the voting-disk majority OPEN(P+2) record. */
static bool test_qvotec_open_present;
static uint64 test_qvotec_open_epoch;
static uint64 test_qvotec_open_generation;
static int test_qvotec_bootstrap_calls;
static bool test_activate_authorized = true;
static bool test_publish_authorized = true;
static ClusterWalPinResult test_walr_begin_result = CLUSTER_WAL_PIN_OK;
static ClusterWalrReleaseResult test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
static int test_walr_begin_calls = 0;
static int test_walr_end_calls = 0;
static uint16 test_walr_thread = 0;
static int test_order_seq = 0;
static int test_walr_begin_order = 0;
static int test_cf_acquire_order = 0;
static int test_cf_release_order = 0;
static int test_last_rename_order = 0;
static TimestampTz test_now = INT64_C(1700000000000000);

typedef struct ClusterWalRootPublishGuard ClusterWalRootPublishGuard;

extern ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *expected_root,
											   bool require_sealed_pin,
											   ClusterWalRootPublishGuard **out_guard);
extern ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard);

void
ExceptionalCondition(const char *conditionName, const char *fileName, int lineNumber)
{
	printf("# Assert failed: %s at %s:%d\n", conditionName, fileName, lineNumber);
	abort();
}

int
OpenTransientFile(const char *fileName, int fileFlags)
{
	return open(fileName, fileFlags, 0600);
}

/* linked by xlogreader_fs.o via libpgport_srv.a path.o (make_absolute_path
 * error paths) — the unit harness never raises; plain open() suffices. */
int
BasicOpenFile(const char *file_name, int fileFlags)
{
	return open(file_name, fileFlags, 0);
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

int
BasicOpenFilePerm(const char *fileName, int fileFlags, mode_t fileMode)
{
	return open(fileName, fileFlags, fileMode);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

int
pg_fsync(int fd)
{
	return fsync(fd);
}

int
durable_rename(const char *oldfile, const char *newfile, int elevel pg_attribute_unused())
{
	test_durable_rename_calls++;
	test_last_rename_order = ++test_order_seq;
	if (test_fail_primary_rename && strstr(newfile, CLUSTER_CONTROL_ROOT_REL_PATH) != NULL
		&& strstr(newfile, ".bak") == NULL) {
		errno = EIO;
		return -1;
	}
	return rename(oldfile, newfile);
}

bool
pg_strong_random(void *buf, size_t len)
{
	static uint8 seed = 0x31;
	uint8 *bytes = buf;
	size_t i;

	for (i = 0; i < len; i++)
		bytes[i] = seed++;
	return true;
}

TimestampTz
GetCurrentTimestamp(void)
{
	return ++test_now;
}

static uint64 test_membership_incarnation = UINT64_C(0x1020304050607080);

uint64
cluster_membership_get_last_admitted_incarnation(int32 node_id)
{
	(void)node_id;
	return test_membership_incarnation;
}

uint16
cluster_wal_thread_id(void)
{
	return test_own_thread;
}

bool
cluster_r4_bit22_cutover_active(void)
{
	return test_bit22_latch_active;
}

bool
cluster_r4_bit22_source_writer_enter(void)
{
	return true;
}

void
cluster_r4_bit22_source_writer_leave(void)
{}

bool
cluster_r4_bit22_source_close_begin(uint64 transition_epoch pg_attribute_unused(),
									uint64 prepare_generation pg_attribute_unused())
{
	return true;
}

static bool test_source_close_current_ok;

bool
cluster_r4_bit22_source_close_current(uint64 transition_epoch pg_attribute_unused(),
									  uint64 prepare_generation pg_attribute_unused())
{
	return test_source_close_current_ok;
}

bool
cluster_r4_bit22_cutover_latch_apply(uint64 transition_epoch, uint64 round_generation)
{
	test_bit22_latch_apply_calls++;
	if (!test_bit22_latch_apply_ok)
		return false;
	test_bit22_latch_active = true;
	test_bit22_latch_apply_epoch = transition_epoch;
	test_bit22_latch_apply_generation = round_generation;
	return true;
}

ClusterSemanticActivationResult
cluster_qvotec_bootstrap_read_semantic_activation(
	uint8 selected[CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES], bool *implicit_open)
{
	test_qvotec_bootstrap_calls++;
	if (selected != NULL)
		memset(selected, 0, CLUSTER_SEMANTIC_ACTIVATION_RECORD_BYTES);
	if (implicit_open != NULL)
		*implicit_open = false;
	if (!test_qvotec_open_present)
		return CLUSTER_SEMANTIC_ACTIVATION_QUORUM_HOLD;
	if (selected != NULL)
		selected[0] = 0x5a; /* non-zero: restore decodes via the stub below */
	if (implicit_open != NULL)
		*implicit_open = true;
	return CLUSTER_SEMANTIC_ACTIVATION_OK;
}

static ClusterSemanticActivationRecord test_decoded_open;

bool
cluster_semantic_activation_record_decode(
	const uint8 bytes[512] pg_attribute_unused(),
	ClusterSemanticActivationRecord *record pg_attribute_unused(),
	ClusterSemanticActivationRefusal *refusal pg_attribute_unused())
{
	if (record != NULL)
		*record = test_decoded_open;
	return true;
}

bool
cluster_r4_bit22_cutover_latch_verify(void)
{
	return test_bit22_latch_active;
}

uint64
GetSystemIdentifier(void)
{
	return test_system_identifier;
}

int
cluster_conf_node_count(void)
{
	return test_node_count;
}

void
cluster_shared_fs_get_storage_uuid(char *out, size_t outlen)
{
	strlcpy(out, test_storage_uuid_text, outlen);
}

ClusterCfContractState
cluster_cf_contract_load(const char *pgdata pg_attribute_unused())
{
	return test_contract;
}

bool
cluster_cf_storage_write_allowed(ClusterCfContractState state, bool multi_node)
{
	return !multi_node || state == CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
}

bool
cluster_cf_storage_probe_local(void)
{
	return test_local_probe;
}

bool
cluster_cf_lock(LOCKMODE mode pg_attribute_unused())
{
	test_cf_lock_calls++;
	test_cf_acquire_order = ++test_order_seq;
	return test_cf_grant;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode pg_attribute_unused())
{
	return test_cf_grant && test_cf_clusterwide;
}

ClusterCfReleaseResult
cluster_cf_unlock_confirmed(LOCKMODE mode pg_attribute_unused())
{
	test_cf_release_order = ++test_order_seq;
	return test_cf_release_confirmed ? CLUSTER_CF_RELEASE_CONFIRMED
									 : CLUSTER_CF_RELEASE_UNCONFIRMED;
}

ClusterWalPinResult
cluster_wal_retention_root_publish_begin_exact(const ClusterControlRootReadToken *expected_root,
											   bool require_sealed_pin pg_attribute_unused(),
											   ClusterWalRootPublishGuard **out_guard)
{
	test_walr_begin_calls++;
	test_walr_thread = expected_root->origin_thread_id;
	test_walr_begin_order = ++test_order_seq;
	if (test_walr_begin_result != CLUSTER_WAL_PIN_OK)
		return test_walr_begin_result;
	*out_guard = (ClusterWalRootPublishGuard *)(uintptr_t)0x1;
	return CLUSTER_WAL_PIN_OK;
}

ClusterWalrReleaseResult
cluster_wal_retention_root_publish_end(ClusterWalRootPublishGuard **guard)
{
	test_walr_end_calls++;
	++test_order_seq;
	if (test_walr_end_result == CLUSTER_WALR_RELEASE_CONFIRMED)
		*guard = NULL;
	return test_walr_end_result;
}

bool
cluster_control_root_create_authority_current_v1(
	const ClusterControlRootMigrationImage *image pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused())
{
	return test_create_authorized;
}

bool
cluster_control_root_activate_authority_current_v1(
	const ClusterControlRootFileToken *expected_token pg_attribute_unused(),
	const uint8 expected_round_sha256[32] pg_attribute_unused(),
	const ClusterControlRootMigrationRoundV1 *round pg_attribute_unused())
{
	return test_activate_authorized;
}

bool
cluster_control_root_publish_authority_current_v1(
	const ClusterControlRootReadToken *expected_token pg_attribute_unused(),
	const ClusterControlRootPatch *patch pg_attribute_unused(),
	ClusterControlRootPublishReason reason pg_attribute_unused())
{
	return test_publish_authorized;
}

static void
put_u16_le(uint8 *dst, uint16 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
}

static void
put_u32_le(uint8 *dst, uint32 value)
{
	dst[0] = (uint8)value;
	dst[1] = (uint8)(value >> 8);
	dst[2] = (uint8)(value >> 16);
	dst[3] = (uint8)(value >> 24);
}

static void
put_u64_le(uint8 *dst, uint64 value)
{
	int i;

	for (i = 0; i < 8; i++) {
		dst[i] = (uint8)value;
		value >>= 8;
	}
}

static uint32
image_crc(const uint8 *bytes, size_t len)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, len);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static void
sha256_bytes(const uint8 *bytes, size_t len, uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);

	if (ctx == NULL || pg_cryptohash_init(ctx) < 0 || pg_cryptohash_update(ctx, bytes, len) < 0
		|| pg_cryptohash_final(ctx, out, PG_SHA256_DIGEST_LENGTH) < 0)
		abort();
	pg_cryptohash_free(ctx);
}

static void
round_sha256(const ClusterControlRootMigrationRoundV1 *round, uint8 out[PG_SHA256_DIGEST_LENGTH])
{
	uint8 bytes[80];

	memset(bytes, 0, sizeof(bytes));
	memcpy(bytes, "PCRM", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 80);
	put_u64_le(bytes + 8, round->prepare_generation);
	put_u64_le(bytes + 16, round->transition_epoch);
	put_u64_le(bytes + 24, round->source_feature_bitmap);
	put_u64_le(bytes + 32, round->target_feature_bitmap);
	put_u64_le(bytes + 40, round->admitted_bitmap_low);
	put_u64_le(bytes + 48, round->admitted_bitmap_high);
	put_u64_le(bytes + 56, round->capability_sample_digest);
	put_u64_le(bytes + 64, round->coordinator_incarnation);
	put_u32_le(bytes + 72, round->coordinator_node_id);
	sha256_bytes(bytes, sizeof(bytes), out);
}

static void
path_for(char *dst, size_t dstlen, const char *rel)
{
	snprintf(dst, dstlen, "%s/%s", test_root, rel);
}

static void
wipe_root_files(void)
{
	char path[MAXPGPATH];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	unlink(path);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	unlink(path);
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	test_node_count = 4;
	test_local_probe = true;
	test_cf_grant = true;
	test_cf_clusterwide = true;
	test_cf_release_confirmed = true;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	test_fail_primary_rename = false;
	test_create_authorized = true;
	test_activate_authorized = true;
	test_publish_authorized = true;
	test_walr_begin_result = CLUSTER_WAL_PIN_OK;
	test_walr_end_result = CLUSTER_WALR_RELEASE_CONFIRMED;
	test_walr_begin_calls = 0;
	test_walr_end_calls = 0;
	test_walr_thread = 0;
	test_order_seq = 0;
	test_walr_begin_order = 0;
	test_cf_acquire_order = 0;
	test_cf_release_order = 0;
	test_last_rename_order = 0;
}

static void
write_all_or_abort(const char *path, const void *buf, size_t len)
{
	const uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);

	if (fd < 0)
		abort();
	while (done < len) {
		ssize_t n = write(fd, bytes + done, len - done);

		if (n <= 0)
			abort();
		done += (size_t)n;
	}
	close(fd);
}

static void
read_all_or_abort(const char *path, void *buf, size_t len)
{
	uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_RDONLY);

	if (fd < 0)
		abort();
	while (done < len) {
		ssize_t n = read(fd, bytes + done, len - done);

		if (n <= 0)
			abort();
		done += (size_t)n;
	}
	close(fd);
}

/*
 * write_minimal_checkpoint_segment -- RF-ROOT P9 verification (contract): build
 * a minimal real WAL segment for thread 1 (tli 1, seg 1 — the
 * build_source_wal_state fixture's checkpoint_redo lives at 0x1000000,
 * segment offset 0) containing one CheckPoint record, so the migration
 * image scan can extract the checkpoint record CRC.  XLogRecord encoding
 * follows the on-disk format (header + payload + CRC over everything but
 * the xl_crc field).
 */
static void
write_minimal_checkpoint_segment(const char *thread_dir)
{
	char path[MAXPGPATH];
	uint8 page[XLOG_BLCKSZ];
	XLogLongPageHeaderData longhdr;
	XLogRecord rec;
	pg_crc32c crc;
	int off;

	memset(page, 0, sizeof(page));
	memset(&longhdr, 0, sizeof(longhdr));
	/* Segment page 0 must carry the long header (offset==0 forces
	 * XLP_LONG_HEADER in XLogReaderValidatePageHeader).  The reader's
	 * system_identifier is 0 in the unit harness, so xlp_sysid stays 0;
	 * segment size and block size must match the reader's. */
	longhdr.std.xlp_magic = XLOG_PAGE_MAGIC;
	longhdr.std.xlp_info = XLP_LONG_HEADER;
	longhdr.std.xlp_tli = 1;
	longhdr.std.xlp_pageaddr = UINT64_C(0x1000000);
	longhdr.xlp_sysid = UINT64_C(0);
	longhdr.xlp_seg_size = wal_segment_size;
	longhdr.xlp_xlog_blcksz = XLOG_BLCKSZ;
	memcpy(page, &longhdr, sizeof(longhdr));

	off = SizeOfXLogLongPHD + SizeOfXLogRecord;
	/* Payload follows the XLogInsert encoding for a pure main-data
	 * record: XLogRecordDataHeaderShort (0xFF + len) + CheckPoint bytes.
	 * The record reader parses these headers, so zeros alone would be
	 * misread as block ids. */
	page[off] = XLR_BLOCK_ID_DATA_SHORT;
	page[off + 1] = (uint8)sizeof(CheckPoint);
	memset(page + off + 2, 0, sizeof(CheckPoint));

	memset(&rec, 0, sizeof(rec));
	rec.xl_tot_len = SizeOfXLogRecord + 2 + sizeof(CheckPoint);
	rec.xl_xid = 1;
	rec.xl_prev = UINT64_C(0x1000000);
	rec.xl_info = XLOG_CHECKPOINT_SHUTDOWN;
	rec.xl_rmid = RM_XLOG_ID;
	/* ValidXLogRecord order: payload first, then header up to (not
	 * including) xl_crc. */
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page + off, 2 + sizeof(CheckPoint));
	COMP_CRC32C(crc, (uint8 *)&rec, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	rec.xl_crc = (uint32)crc;
	memcpy(page + SizeOfXLogLongPHD, &rec, sizeof(rec));

	snprintf(path, sizeof(path), "%s/%s", thread_dir, "000000010000000000000001");
	write_all_or_abort(path, page, sizeof(page));
}

static void
build_source_wal_state(void)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateHeader header;
	ClusterWalStateSlot slot;
	ClusterWalThreadClaim claim;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];

	memset(bytes, 0, sizeof(bytes));
	cluster_wal_state_header_fill(&header, INT64_C(1699999999000000));
	memcpy(bytes, &header, sizeof(header));
	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_STOPPED, 1,
								INT64_C(1699999999000001), INT64_C(1699999999000002),
								UINT64_C(0x1000000), 1);
	slot.checkpoint_redo_lsn = UINT64_C(0x1000000);
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	write_all_or_abort(path, bytes, sizeof(bytes));
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
	snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root);
	if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
		abort();
	snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	write_all_or_abort(path, &claim, sizeof(claim));
	write_minimal_checkpoint_segment(thread_dir);
}

static void
fill_identity(ClusterControlRootIdentity *identity)
{
	ClusterWalThreadClaim claim;
	int i;

	memset(identity, 0, sizeof(*identity));
	identity->system_identifier = TEST_SYSID;
	for (i = 0; i < 16; i++) {
		identity->storage_uuid[i] = (uint8)(i * 0x11);
		identity->authority_uuid[i] = (uint8)(0xa0 + i);
	}
	identity->authority_uuid[6] = 0x46;
	identity->authority_uuid[8] = 0x8a;
	identity->origin_thread_id = 1;
	identity->origin_node_id = 0;
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
	identity->thread_claim_created_at = claim.created_at;
	identity->thread_claim_crc32c = claim.crc;
	identity->origin_owner_incarnation = UINT64_C(0x1122334455667788);
	identity->root_lineage_seq = 1;
}

static void
build_migration(ClusterControlRootMigrationImage *image, ClusterControlRootMigrationRoundV1 *round)
{
	ClusterControlRootSnapshot *record;

	memset(image, 0, sizeof(*image));
	image->system_identifier = TEST_SYSID;
	fill_identity(&image->records[0].identity);
	memcpy(image->storage_uuid, image->records[0].identity.storage_uuid, 16);
	memcpy(image->authority_uuid, image->records[0].identity.authority_uuid, 16);
	image->created_at_usec = INT64_C(1699999999000002);
	image->assigned_record_count = 1;
	record = &image->records[0];
	record->lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	record->root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
		  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID | CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
	record->root_publish_seq = 1;
	record->checkpoint_tli = 1;
	record->tail_tli = 1;
	record->recovered_tli = 1;
	record->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	record->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	record->checkpoint_lower_lsn = UINT64_C(0x1000000);
	record->validated_tail_lsn_exclusive = UINT64_C(0x1000000);
	record->recovered_through_lsn_exclusive = UINT64_C(0x1000000);
	record->published_at_usec = image->created_at_usec;
	record->checkpoint_record_crc32c = UINT32_C(0x33445566);
	record->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;

	memset(round, 0, sizeof(*round));
	memcpy(round->magic, "PCRM", 4);
	round->version = 1;
	round->bytes = sizeof(*round);
	round->prepare_generation = 1;
	round->transition_epoch = 7;
	round->target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round->admitted_bitmap_low = 1;
	round->capability_sample_digest = UINT64_C(0x8877665544332211);
	round->coordinator_incarnation = UINT64_C(0x7766554433221100);
	round->coordinator_node_id = 0;
}

static bool
parse_u64_arg(const char *text, uint64 *out)
{
	char *end = NULL;
	unsigned long long value;

	if (text == NULL || text[0] == '\0' || text[0] == '-')
		return false;
	errno = 0;
	value = strtoull(text, &end, 10);
	if (errno != 0 || end == text || *end != '\0')
		return false;
	*out = (uint64)value;
	return true;
}

static int
hex_digit(unsigned char ch)
{
	if (ch >= '0' && ch <= '9')
		return ch - '0';
	if (ch >= 'a' && ch <= 'f')
		return ch - 'a' + 10;
	if (ch >= 'A' && ch <= 'F')
		return ch - 'A' + 10;
	return -1;
}

static bool
parse_uuid_hex(const char *text, uint8 out[16])
{
	int i;

	if (text == NULL || strlen(text) != 32)
		return false;
	for (i = 0; i < 16; i++) {
		int high = hex_digit((unsigned char)text[i * 2]);
		int low = hex_digit((unsigned char)text[i * 2 + 1]);

		if (high < 0 || low < 0)
			return false;
		out[i] = (uint8)((high << 4) | low);
	}
	return true;
}

static bool
read_exact_file(const char *path, void *buf, size_t len)
{
	uint8 *bytes = buf;
	struct stat st;
	size_t done = 0;
	int fd = open(path, O_RDONLY | PG_BINARY);

	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != (off_t)len) {
		if (fd >= 0)
			close(fd);
		return false;
	}
	while (done < len) {
		ssize_t n = read(fd, bytes + done, len - done);

		if (n <= 0) {
			close(fd);
			return false;
		}
		done += (size_t)n;
	}
	return close(fd) == 0;
}

static bool
write_exact_durable(const char *path, const void *buf, size_t len)
{
	const uint8 *bytes = buf;
	size_t done = 0;
	int fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | PG_BINARY, 0600);

	if (fd < 0)
		return false;
	while (done < len) {
		ssize_t n = write(fd, bytes + done, len - done);

		if (n <= 0) {
			close(fd);
			return false;
		}
		done += (size_t)n;
	}
	if (fsync(fd) != 0 || close(fd) != 0)
		return false;
	return true;
}

static bool
fixture_seed_source(uint32 tli, uint64 checkpoint_lsn, uint64 tail_lsn,
					ClusterWalThreadClaim *out_claim)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateSlot slot;
	uint16 bad_thread = 0;
	const char *reason = NULL;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];
	int64 claim_created_at = INT64_C(1700000000000001);

	if (snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME) <= 0
		|| !read_exact_file(path, bytes, sizeof(bytes))
		|| !cluster_wal_state_image_validate(bytes, sizeof(bytes), &bad_thread, &reason))
		return false;
	if (!cluster_wal_state_slot_is_zero(
			(ClusterWalStateSlot *)(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1))))
		return false;

	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_STOPPED, tli, claim_created_at,
								claim_created_at + 1, tail_lsn, 1);
	slot.checkpoint_redo_lsn = checkpoint_lsn;
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	if (!write_exact_durable(path, bytes, sizeof(bytes)))
		return false;

	cluster_wal_thread_claim_fill(out_claim, 1, 0, claim_created_at);
	if (snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root) <= 0)
		return false;
	if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
		return false;
	if (snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME) <= 0
		|| !write_exact_durable(path, out_claim, sizeof(*out_claim)))
		return false;
	return true;
}

static int
fixture_root_main(int argc, char **argv)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootIdentity expected_identity;
	ClusterControlRootReadToken read_token;
	ClusterWalThreadClaim claim;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	uint64 sysid;
	uint64 tli64;
	uint64 checkpoint_lsn;
	uint64 tail_lsn;
	uint32 lifecycle;
	int i;

	if (argc != 10 || strcmp(argv[1], "--fixture-root") != 0 || !parse_u64_arg(argv[4], &sysid)
		|| sysid == 0 || !parse_u64_arg(argv[7], &tli64) || tli64 == 0 || tli64 > UINT32_MAX
		|| !parse_u64_arg(argv[8], &checkpoint_lsn) || checkpoint_lsn == 0
		|| !parse_u64_arg(argv[9], &tail_lsn) || tail_lsn < checkpoint_lsn
		|| strlen(argv[2]) >= sizeof(test_root) || strlen(argv[3]) >= sizeof(test_wal_root)
		|| strlen(argv[5]) != 32) {
		fprintf(stderr, "invalid --fixture-root arguments\n");
		return 2;
	}
	if (strcmp(argv[6], "OPEN") == 0)
		lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	else if (strcmp(argv[6], "RECOVERY_REQUIRED") == 0)
		lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	else if (strcmp(argv[6], "RECOVERY_COMPLETE") == 0)
		lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	else {
		fprintf(stderr, "unsupported fixture lifecycle\n");
		return 2;
	}

	strlcpy(test_root, argv[2], sizeof(test_root));
	strlcpy(test_wal_root, argv[3], sizeof(test_wal_root));
	strlcpy(test_storage_uuid_text, argv[5], sizeof(test_storage_uuid_text));
	test_system_identifier = sysid;
	cluster_shared_data_dir = test_root;
	cluster_wal_threads_dir = test_wal_root;
	DataDir = test_root;
	test_node_count = 1;
	test_local_probe = true;
	if (!fixture_seed_source((uint32)tli64, checkpoint_lsn, tail_lsn, &claim)) {
		fprintf(stderr, "cannot seed canonical stopped WAL source\n");
		return 1;
	}

	memset(&image, 0, sizeof(image));
	image.system_identifier = sysid;
	if (!parse_uuid_hex(argv[5], image.storage_uuid)) {
		fprintf(stderr, "invalid storage UUID\n");
		return 2;
	}
	for (i = 0; i < 16; i++)
		image.authority_uuid[i] = (uint8)(0xa0 + i);
	image.authority_uuid[6] = 0x46;
	image.authority_uuid[8] = 0x8a;
	image.created_at_usec = INT64_C(1700000000000002);
	image.assigned_record_count = 1;

	snapshot = (ClusterControlRootSnapshot){ 0 };
	snapshot.identity.system_identifier = sysid;
	memcpy(snapshot.identity.storage_uuid, image.storage_uuid, 16);
	memcpy(snapshot.identity.authority_uuid, image.authority_uuid, 16);
	snapshot.identity.origin_thread_id = 1;
	snapshot.identity.origin_node_id = 0;
	snapshot.identity.thread_claim_created_at = claim.created_at;
	snapshot.identity.thread_claim_crc32c = claim.crc;
	snapshot.identity.origin_owner_incarnation = UINT64_C(0x1122334455667788);
	snapshot.identity.root_lineage_seq = 1;
	snapshot.lifecycle = lifecycle;
	snapshot.root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						  | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
						  | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	snapshot.root_publish_seq = 1;
	snapshot.checkpoint_tli = (uint32)tli64;
	snapshot.tail_tli = (uint32)tli64;
	snapshot.checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	snapshot.tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	snapshot.checkpoint_lower_lsn = checkpoint_lsn;
	snapshot.validated_tail_lsn_exclusive = tail_lsn;
	snapshot.checkpoint_record_crc32c = UINT32_C(0x33445566);
	if (tail_lsn > checkpoint_lsn) {
		snapshot.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
		snapshot.tail_last_record_lsn = tail_lsn - 1;
		snapshot.tail_last_record_crc32c = UINT32_C(0x55667788);
	}
	if (lifecycle == CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED) {
		snapshot.root_flags |= CLUSTER_CONTROL_ROOT_FLAG_RECOVERED_VALID;
		snapshot.recovered_tli = (uint32)tli64;
		snapshot.recovered_through_lsn_exclusive = checkpoint_lsn;
	}
	snapshot.published_at_usec = image.created_at_usec;
	snapshot.lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
	image.records[0] = snapshot;

	memset(&round, 0, sizeof(round));
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = 1;
	round.transition_epoch = 1;
	round.target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
								  | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = 1;
	round.capability_sample_digest = UINT64_C(0x8877665544332211);
	round.coordinator_incarnation = UINT64_C(0x7766554433221100);
	round.coordinator_node_id = 0;

	wipe_root_files();
	if (cluster_control_root_create_prepared(&image, &round, &prepared)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root prepare failed\n");
		return 1;
	}
	round_sha256(&round, round_sha);
	if (cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| active.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE) {
		fprintf(stderr, "control-root activation verification failed\n");
		return 1;
	}
	expected_identity = snapshot.identity;
	if (cluster_control_root_read_canonical(1, &expected_identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
											&snapshot, &read_token)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root activation verification failed\n");
		return 1;
	}
	return 0;
}

/*
 * RF-ROOT P6 pair cast (t/243 setup producer).
 *
 * Unlike the synthetic --fixture-root mode, this mode never rewrites the
 * wal-state registry or claim files.  It reads the REAL stopped slots for
 * threads 1 and 2 and the REAL claim files, and mints a canonical control
 * root whose two records mirror the whole registry:
 *
 *   record[0] = thread 1 / node 0, lifecycle argv[9]
 *   record[1] = thread 2 / node 1, lifecycle argv[7]
 *
 * argv: --fixture-root-cast <shared_root> <wal_root> <sysid>
 *       <storage_uuid_hex32> <authority_uuid_hex32> <lifecycle2> <inc2>
 *       <lifecycle1> <inc1>
 */
static bool
fixture_cast_load_thread(uint16 thread_id, int32 node_id, uint32 *out_tli, uint64 *out_ckpt,
						 uint64 *out_tail, ClusterWalThreadClaim *out_claim)
{
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateSlot slot;
	ClusterWalThreadClaim disk_claim;
	ClusterWalThreadClaim expected_claim;
	uint16 bad_thread = 0;
	const char *reason = NULL;
	char path[MAXPGPATH];
	char thread_dir[MAXPGPATH];

	if (snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME) <= 0
		|| !read_exact_file(path, bytes, sizeof(bytes))
		|| !cluster_wal_state_image_validate(bytes, sizeof(bytes), &bad_thread, &reason))
		return false;
	memcpy(&slot, bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(thread_id), sizeof(slot));
	if (cluster_wal_state_slot_classify(&slot, thread_id, -1, NULL) != CLUSTER_WAL_SLOT_OK
		|| slot.state != CLUSTER_WAL_SLOT_STATE_STOPPED || slot.node_id != node_id || slot.tli == 0
		|| slot.checkpoint_redo_lsn == 0 || slot.highest_lsn == 0
		|| slot.highest_lsn < slot.checkpoint_redo_lsn || slot.merge_recovered_lsn != 0)
		return false;

	if (snprintf(thread_dir, sizeof(thread_dir), "%s/thread_%u", test_wal_root, thread_id) <= 0
		|| snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME) <= 0
		|| !read_exact_file(path, (uint8 *)&disk_claim, sizeof(disk_claim)))
		return false;
	cluster_wal_thread_claim_fill(&expected_claim, thread_id, node_id, disk_claim.created_at);
	if (disk_claim.magic != expected_claim.magic || disk_claim.version != expected_claim.version
		|| disk_claim.thread_id != thread_id || disk_claim.node_id != node_id
		|| disk_claim.created_at == 0 || disk_claim.crc != expected_claim.crc)
		return false;

	*out_tli = slot.tli;
	*out_ckpt = slot.checkpoint_redo_lsn;
	*out_tail = slot.highest_lsn;
	*out_claim = expected_claim;
	return true;
}

static void
fixture_cast_fill_record(ClusterControlRootSnapshot *snapshot, uint64 sysid,
						 const uint8 storage_uuid[16], const uint8 authority_uuid[16],
						 uint16 thread_id, int32 node_id, const ClusterWalThreadClaim *claim,
						 uint32 lifecycle, uint64 owner_incarnation, uint32 tli, uint64 ckpt,
						 uint64 tail)
{
	*snapshot = (ClusterControlRootSnapshot){ 0 };
	snapshot->identity.system_identifier = sysid;
	memcpy(snapshot->identity.storage_uuid, storage_uuid, 16);
	memcpy(snapshot->identity.authority_uuid, authority_uuid, 16);
	snapshot->identity.origin_thread_id = thread_id;
	snapshot->identity.origin_node_id = node_id;
	snapshot->identity.thread_claim_created_at = claim->created_at;
	snapshot->identity.thread_claim_crc32c = claim->crc;
	snapshot->identity.origin_owner_incarnation = owner_incarnation;
	snapshot->identity.root_lineage_seq = 1;
	snapshot->lifecycle = lifecycle;
	snapshot->lifecycle_reason = CLUSTER_CONTROL_ROOT_PUBLISH_MIGRATION_IMPORT;
	snapshot->root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
						   | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID
						   | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	snapshot->root_publish_seq = 1;
	snapshot->checkpoint_tli = tli;
	snapshot->tail_tli = tli;
	snapshot->checkpoint_source_kind = CLUSTER_CONTROL_ROOT_CHECKPOINT_NATIVE_V1;
	snapshot->tail_validation_kind = CLUSTER_CONTROL_ROOT_TAIL_WAL_RECORD_SCAN_V1;
	snapshot->checkpoint_lower_lsn = ckpt;
	snapshot->validated_tail_lsn_exclusive = tail;
	snapshot->checkpoint_record_crc32c = UINT32_C(0x33445566);
	if (tail > ckpt) {
		snapshot->root_flags |= CLUSTER_CONTROL_ROOT_FLAG_TAIL_LAST_RECORD_VALID;
		snapshot->tail_last_record_lsn = tail - 1;
		snapshot->tail_last_record_crc32c = UINT32_C(0x55667788);
	}
}

static int
fixture_cast_main(int argc, char **argv)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootIdentity expected_identity;
	ClusterControlRootReadToken read_token;
	ClusterWalThreadClaim claim1;
	ClusterWalThreadClaim claim2;
	ClusterControlRootResult result_cast_prepare;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	uint64 sysid;
	uint64 inc1;
	uint64 inc2;
	uint32 lifecycle1;
	uint32 lifecycle2;
	uint32 tli1;
	uint32 tli2;
	uint64 ckpt1;
	uint64 ckpt2;
	uint64 tail1;
	uint64 tail2;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];

	if (argc != 11 || strcmp(argv[1], "--fixture-root-cast") != 0 || !parse_u64_arg(argv[4], &sysid)
		|| sysid == 0 || strlen(argv[2]) >= sizeof(test_root)
		|| strlen(argv[3]) >= sizeof(test_wal_root) || strlen(argv[5]) != 32
		|| strlen(argv[6]) != 32 || !parse_uuid_hex(argv[5], storage_uuid)
		|| !parse_uuid_hex(argv[6], authority_uuid) || (authority_uuid[6] & 0xf0) != 0x40
		|| (authority_uuid[8] & 0xc0) != 0x80 || !parse_u64_arg(argv[8], &inc2) || inc2 == 0
		|| !parse_u64_arg(argv[10], &inc1) || inc1 == 0) {
		fprintf(stderr, "invalid --fixture-root-cast arguments\n");
		return 2;
	}
	if (strcmp(argv[7], "RECOVERY_COMPLETE") == 0)
		lifecycle2 = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	else if (strcmp(argv[7], "OPEN") == 0)
		lifecycle2 = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	else if (strcmp(argv[7], "RECOVERY_REQUIRED") == 0)
		lifecycle2 = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	else {
		fprintf(stderr, "unsupported cast lifecycle 2\n");
		return 2;
	}
	if (strcmp(argv[9], "OPEN") == 0)
		lifecycle1 = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	else if (strcmp(argv[9], "RECOVERY_COMPLETE") == 0)
		lifecycle1 = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	else {
		fprintf(stderr, "unsupported cast lifecycle 1\n");
		return 2;
	}

	strlcpy(test_root, argv[2], sizeof(test_root));
	strlcpy(test_wal_root, argv[3], sizeof(test_wal_root));
	strlcpy(test_storage_uuid_text, argv[5], sizeof(test_storage_uuid_text));
	test_system_identifier = sysid;
	cluster_shared_data_dir = test_root;
	cluster_wal_threads_dir = test_wal_root;
	DataDir = test_root;
	test_node_count = 2;
	test_local_probe = true;

	if (!fixture_cast_load_thread(1, 0, &tli1, &ckpt1, &tail1, &claim1)) {
		fprintf(stderr, "cannot load real thread-1 source\n");
		return 1;
	}
	if (!fixture_cast_load_thread(2, 1, &tli2, &ckpt2, &tail2, &claim2)) {
		fprintf(stderr, "cannot load real thread-2 source\n");
		return 1;
	}

	memset(&image, 0, sizeof(image));
	image.system_identifier = sysid;
	memcpy(image.storage_uuid, storage_uuid, 16);
	memcpy(image.authority_uuid, authority_uuid, 16);
	image.created_at_usec = INT64_C(1700000000000002);
	image.assigned_record_count = 2;
	fixture_cast_fill_record(&image.records[0], sysid, storage_uuid, authority_uuid, 1, 0, &claim1,
							 lifecycle1, inc1, tli1, ckpt1, tail1);
	fixture_cast_fill_record(&image.records[1], sysid, storage_uuid, authority_uuid, 2, 1, &claim2,
							 lifecycle2, inc2, tli2, ckpt2, tail2);

	memset(&round, 0, sizeof(round));
	memcpy(round.magic, "PCRM", 4);
	round.version = 1;
	round.bytes = sizeof(round);
	round.prepare_generation = 1;
	round.transition_epoch = 1;
	round.target_feature_bitmap = PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
								  | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1;
	round.admitted_bitmap_low = 3;
	round.capability_sample_digest = UINT64_C(0x8877665544332211);
	round.coordinator_incarnation = UINT64_C(0x7766554433221100);
	round.coordinator_node_id = 0;

	wipe_root_files();
	result_cast_prepare = cluster_control_root_create_prepared(&image, &round, &prepared);
	if (result_cast_prepare != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root cast prepare failed (result %d)\n", (int)result_cast_prepare);
		return 1;
	}
	round_sha256(&round, round_sha);
	if (cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| active.activation_state != CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE) {
		fprintf(stderr, "control-root cast activation failed\n");
		return 1;
	}
	expected_identity = image.records[0].identity;
	if (cluster_control_root_read_canonical(1, &expected_identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
											&snapshot, &read_token)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root cast thread-1 readback failed\n");
		return 1;
	}
	expected_identity = image.records[1].identity;
	if (cluster_control_root_read_canonical(2, &expected_identity, CLUSTER_CONTROL_ROOT_READ_STRONG,
											&snapshot, &read_token)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		fprintf(stderr, "control-root cast thread-2 readback failed\n");
		return 1;
	}
	return 0;
}

static ClusterControlRootResult
create_prepared(ClusterControlRootMigrationImage *image, ClusterControlRootMigrationRoundV1 *round,
				ClusterControlRootFileToken *token)
{
	build_migration(image, round);
	return cluster_control_root_create_prepared(image, round, token);
}

static void
force_first_record_lineage(uint64 lineage)
{
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	uint8 *record = bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	put_u64_le(record + 24, lineage);
	put_u32_le(record + 504, image_crc(record, 504));
	put_u32_le(bytes + 96, image_crc(bytes + CLUSTER_CONTROL_ROOT_HEADER_BYTES,
									 sizeof(bytes) - CLUSTER_CONTROL_ROOT_HEADER_BYTES));
	put_u32_le(bytes + 504, image_crc(bytes, 504));
	write_all_or_abort(primary, bytes, sizeof(bytes));
	write_all_or_abort(bak, bytes, sizeof(bytes));
}

static void
build_owner_rejoin_patch(const ClusterControlRootSnapshot *snapshot, uint64 new_incarnation,
						 uint64 new_lineage, ClusterControlRootPatch *patch)
{
	memset(patch, 0, sizeof(*patch));
	patch->mask = UINT64_C(0x3b);
	patch->expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch->desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch->desired.identity.origin_owner_incarnation = new_incarnation;
	patch->desired.identity.root_lineage_seq = new_lineage;
	patch->desired.root_flags = snapshot->root_flags;
	patch->desired.checkpoint_tli = snapshot->checkpoint_tli;
	patch->desired.checkpoint_source_kind = snapshot->checkpoint_source_kind;
	patch->desired.checkpoint_lower_lsn = snapshot->checkpoint_lower_lsn;
	patch->desired.checkpoint_record_crc32c = snapshot->checkpoint_record_crc32c;
	patch->desired.tail_tli = snapshot->tail_tli;
	patch->desired.tail_validation_kind = snapshot->tail_validation_kind;
	patch->desired.validated_tail_lsn_exclusive = snapshot->validated_tail_lsn_exclusive;
	patch->desired.tail_last_record_lsn = snapshot->tail_last_record_lsn;
	patch->desired.tail_last_record_crc32c = snapshot->tail_last_record_crc32c;
	patch->desired.recovered_tli = snapshot->recovered_tli;
	patch->desired.recovered_through_lsn_exclusive = snapshot->recovered_through_lsn_exclusive;
	patch->desired.recovered_last_record_lsn = snapshot->recovered_last_record_lsn;
	patch->desired.recovered_last_record_crc32c = snapshot->recovered_last_record_crc32c;
}

static void
setup_fixture(void)
{
	char tmpl[MAXPGPATH];
	char path[MAXPGPATH];

	strlcpy(tmpl, "/tmp/pgrac_control_root_XXXXXX", sizeof(tmpl));
	if (mkdtemp(tmpl) == NULL)
		abort();
	strlcpy(test_root, tmpl, sizeof(test_root));
	cluster_shared_data_dir = test_root;
	DataDir = test_root;

	snprintf(path, sizeof(path), "%s/global", test_root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(test_wal_root, sizeof(test_wal_root), "%s/wal", test_root);
	if (mkdir(test_wal_root, 0700) != 0)
		abort();
	cluster_wal_threads_dir = test_wal_root;
	build_source_wal_state();
}

UT_TEST(test_abi_identity_and_features)
{
	ClusterControlRootIdentity left;
	ClusterControlRootIdentity right;
	uint64 known = (UINT64_C(1) << 0) | PGRAC_CONTROL_ROOT_FEATURE_WAL_REUSE_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_PAGE_STABLE_BASE_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_SPACE_METADATA_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_CONSERVATIVE_COMMIT_SCN_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1
				   | PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;

	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FILE_BYTES, 66048);
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FORMAT_FLAGS_V1, UINT64_C(0x0d));
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_FLAGS_V1, UINT32_C(0x1fd));
	UT_ASSERT_EQ(CLUSTER_CONTROL_ROOT_PATCH_ALL_V1, UINT64_C(0xfb));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_DUTY_IDENTITY_V1, UINT64_C(0x00400000));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1, UINT64_C(0x00800000));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1, UINT64_C(0x01000000));
	UT_ASSERT_EQ(PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1, UINT64_C(0x01ee0001));
	UT_ASSERT_EQ(known, PGRAC_CONTROL_ROOT_FEATURE_KNOWN_MASK_V1);
	fill_identity(&left);
	right = left;
	UT_ASSERT(cluster_control_root_identity_equal(&left, &right));
	right.root_lineage_seq++;
	UT_ASSERT(!cluster_control_root_identity_equal(&left, &right));
	UT_ASSERT(cluster_control_root_feature_bitmap_is_known(known));
	UT_ASSERT(!cluster_control_root_feature_bitmap_is_known(UINT64_C(1) << 63));
}

UT_TEST(test_invalid_argument_precedes_authority_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	round.reserved76 = 1;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_external_fence_bit24_activation_is_forbidden_without_provider)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	round.target_feature_bitmap |= PGRAC_CONTROL_ROOT_FEATURE_RECOVERY_SERIAL_V1
								   | PGRAC_CONTROL_ROOT_FEATURE_EXTERNAL_FENCE_V1;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_create_and_read_primary)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;
	uint8 primary[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	uint8 bak[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	char primary_path[MAXPGPATH];
	char bak_path[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(file_token.file_txn_seq, 1);
	UT_ASSERT_EQ(file_token.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
	path_for(primary_path, sizeof(primary_path), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak_path, sizeof(bak_path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary_path, primary, sizeof(primary));
	read_all_or_abort(bak_path, bak, sizeof(bak));
	UT_ASSERT(memcmp(primary, bak, sizeof(primary)) == 0);
	UT_ASSERT_EQ(image_crc(primary + CLUSTER_CONTROL_ROOT_HEADER_BYTES,
						   sizeof(primary) - CLUSTER_CONTROL_ROOT_HEADER_BYTES),
				 file_token.body_crc32c);

	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&snapshot.identity, &image.records[0].identity));
	UT_ASSERT_EQ(read_token.file_txn_seq, 1);
	UT_ASSERT_EQ(read_token.origin_thread_id, 1);
}

UT_TEST(test_bootstrap_read_never_returns_authority_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_lock_calls = 0;
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(
					 1, NULL, CLUSTER_CONTROL_ROOT_READ_BOOTSTRAP_VALIDATE, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

/* RF-ROOT P7 (contract §A / follow-up E1): the NULL-identity bug class — a
 * STRONG read with expected_identity == NULL must stay INVALID_ARGUMENT=23
 * (the G1b step-4 sites' inertness signature).  The legal no-prior-identity
 * path is the two-step discovered read below. */
UT_TEST(test_round_sha256_is_deterministic_and_matches_create)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	uint8 sha_a[PG_SHA256_DIGEST_LENGTH];
	uint8 sha_b[PG_SHA256_DIGEST_LENGTH];

	wipe_root_files();
	build_migration(&image, &round);
	UT_ASSERT(cluster_control_root_round_sha256(&round, sha_a));
	UT_ASSERT(cluster_control_root_round_sha256(&round, sha_b));
	UT_ASSERT(memcmp(sha_a, sha_b, sizeof(sha_a)) == 0);
	/* create_prepared must succeed with the same round (its header stores
	 * the same wire-encoded sha). */
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
}

UT_TEST(test_build_migration_image_maps_registry_and_claims)
{
	ClusterControlRootMigrationImage image;

	wipe_root_files();
	build_source_wal_state(); /* registry slot 1 STOPPED + thread_1 claim */
	test_membership_incarnation = UINT64_C(0x1020304050607080);
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(NULL, &image),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(image.assigned_record_count, 1);
	/* the checkpoint record CRC must come from the real WAL stream scan */
	UT_ASSERT(image.records[0].checkpoint_record_crc32c != 0);
	UT_ASSERT_EQ(image.records[0].identity.origin_thread_id, 1);
	UT_ASSERT_EQ(image.records[0].identity.origin_node_id, 0);
	UT_ASSERT_EQ(image.records[0].identity.origin_owner_incarnation, UINT64_C(0x1020304050607080));
	UT_ASSERT_EQ(image.records[0].identity.thread_claim_created_at, INT64_C(1699999999000001));
	UT_ASSERT_EQ(image.records[0].lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED);
	UT_ASSERT_EQ(image.records[0].checkpoint_lower_lsn, UINT64_C(0x1000000));
	UT_ASSERT_EQ(image.records[0].validated_tail_lsn_exclusive, UINT64_C(0x1000000));
	UT_ASSERT((image.records[0].root_flags & CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID) != 0);
	UT_ASSERT(memcmp(image.storage_uuid, image.records[0].identity.storage_uuid, 16) == 0);
	build_source_wal_state(); /* restore the shared fixture for later tests */
}

UT_TEST(test_build_migration_image_accepts_frozen_active_slot)
{
	/* RF-ROOT P9 verification (implementation): the online first-open round freezes
	 * every member's wal-state writers first; an ACTIVE slot is then
	 * provably quiesced and acceptable as migration input. */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	uint8 bytes[CLUSTER_WAL_STATE_FILE_SIZE];
	ClusterWalStateSlot slot;

	wipe_root_files();
	memset(bytes, 0, sizeof(bytes));
	cluster_wal_state_header_fill((ClusterWalStateHeader *)bytes, INT64_C(1699999999000000));
	cluster_wal_state_slot_fill(&slot, 1, 0, CLUSTER_WAL_SLOT_STATE_ACTIVE, 1,
								INT64_C(1699999999000001), INT64_C(1699999999000002),
								UINT64_C(0x1000000), 1);
	slot.checkpoint_redo_lsn = UINT64_C(0x1000000);
	slot.crc = cluster_wal_state_block_crc(&slot);
	memcpy(bytes + CLUSTER_WAL_STATE_SLOT_OFFSET(1), &slot, sizeof(slot));
	{
		char path[MAXPGPATH];

		snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
		write_all_or_abort(path, bytes, sizeof(bytes));
	}
	/* claim + minimal WAL segment for the scan */
	{
		char thread_dir[MAXPGPATH];
		char path[MAXPGPATH];
		ClusterWalThreadClaim claim;

		snprintf(thread_dir, sizeof(thread_dir), "%s/thread_1", test_wal_root);
		if (mkdir(thread_dir, 0700) != 0 && errno != EEXIST)
			abort();
		cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000001));
		snprintf(path, sizeof(path), "%s/%s", thread_dir, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
		write_all_or_abort(path, &claim, sizeof(claim));
		write_minimal_checkpoint_segment(thread_dir);
	}
	test_membership_incarnation = UINT64_C(0x1020304050607080);
	memset(&round, 0, sizeof(round));
	round.transition_epoch = 7;
	round.prepare_generation = 5;

	/* ACTIVE without the round's freeze -> refused. */
	test_source_close_current_ok = false;
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(&round, &image),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);

	/* ACTIVE frozen by this exact round -> accepted. */
	test_source_close_current_ok = true;
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(&round, &image),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(image.assigned_record_count, 1);
	UT_ASSERT_EQ(image.records[0].identity.origin_thread_id, 1);
	UT_ASSERT(image.records[0].checkpoint_record_crc32c != 0);
	test_source_close_current_ok = false;
}

UT_TEST(test_build_migration_image_rejects_non_stopped_slot)
{
	ClusterControlRootMigrationImage image;
	ClusterWalStateSlot slot;
	char path[MAXPGPATH];
	int fd;

	wipe_root_files();
	build_source_wal_state();
	/* flip slot 1 to ACTIVE — the W6 CLOSED precondition is violated */
	path_for(path, sizeof(path), ""); /* reuse: write into the wal root */
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	memcpy(&slot, (void *)0, 0); /* noop to keep compiler quiet */
	{
		ClusterWalStateSlot s;

		if (pread(fd, &s, sizeof(s), CLUSTER_WAL_STATE_SLOT_OFFSET(1)) != (ssize_t)sizeof(s))
			abort();
		s.state = CLUSTER_WAL_SLOT_STATE_ACTIVE;
		s.crc = cluster_wal_state_block_crc(&s);
		if (pwrite(fd, &s, sizeof(s), CLUSTER_WAL_STATE_SLOT_OFFSET(1)) != (ssize_t)sizeof(s))
			abort();
	}
	close(fd);
	UT_ASSERT_EQ(cluster_control_root_build_migration_image(NULL, &image),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	build_source_wal_state(); /* restore the STOPPED fixture */
}

UT_TEST(test_strong_read_null_identity_stays_invalid_argument)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, NULL, CLUSTER_CONTROL_ROOT_READ_STRONG,
													 &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_discovered_read_binds_identity_and_mints_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(1, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&snapshot.identity, &image.records[0].identity));
	UT_ASSERT_EQ(snapshot.checkpoint_lower_lsn, UINT64_C(0x1000000));
	/* The STRONG step mints the authority token (BOOTSTRAP never does). */
	UT_ASSERT_EQ(read_token.file_txn_seq, 1);
	UT_ASSERT_EQ(read_token.origin_thread_id, 1);
}

UT_TEST(test_discovered_read_absent_thread_fails_closed)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	/* The fixture mints record[0] only; tid 2 was never present. */
	UT_ASSERT_EQ(cluster_control_root_read_canonical_discovered(2, &snapshot, &read_token),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_valid_bak_blocks_corrupt_primary)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken read_token;
	char path[MAXPGPATH];
	int fd;
	uint8 byte;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(pread(fd, &byte, 1, 0), 1);
	byte ^= 0xff;
	UT_ASSERT_EQ(pwrite(fd, &byte, 1, 0), 1);
	close(fd);
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&read_token, 0xee, sizeof(read_token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(read_token.file_txn_seq, 0);
}

UT_TEST(test_storage_contract_fails_before_cf_or_file_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
	UT_ASSERT_EQ(create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
}

UT_TEST(test_single_node_local_probe_fails_before_cf_or_file_io)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	test_node_count = 1;
	test_local_probe = false;
	UT_ASSERT_EQ(create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
}

UT_TEST(test_activate_and_stale_token)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterControlRootFileToken stale_out;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	UT_ASSERT_EQ(active.file_txn_seq, 2);
	memset(&stale_out, 0xee, sizeof(stale_out));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &stale_out),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(stale_out.file_txn_seq, 0);
}

UT_TEST(test_restore_bit22_latch_from_active_root)
{
	/* RF-ROOT P9 verification (implementation): the latch restores only on the
	 * DURABLE Target OPEN proof — a strict-majority OPEN(P+2) record on
	 * the voting disks cross-matched to the ACTIVE canonical root's round
	 * identity (root migration_transition_epoch == OPEN.transition_epoch
	 * AND root migration_prepare_generation + 2 == OPEN.record_generation).
	 * No record-lifecycle axis participates.  The apply lands at
	 * TARGET_BOOTSTRAP; a refused apply (census RED) fails closed. */
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];

	/* No durable OPEN record -> no restore. */
	wipe_root_files();
	test_bit22_latch_active = false;
	test_bit22_latch_apply_calls = 0;
	test_qvotec_open_present = false;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* OPEN record present but no root -> no restore. */
	test_qvotec_open_present = true;
	test_qvotec_open_epoch = 7;
	test_qvotec_open_generation = 7;
	test_decoded_open.phase = CLUSTER_SEMANTIC_PHASE_OPEN;
	test_decoded_open.transition_epoch = 7;
	test_decoded_open.record_generation = 7;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* PREPARED root (create only) -> no restore (not ACTIVE). */
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* ACTIVE root but the OPEN record does not cross-match the round
	 * identity -> no restore. */
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	test_decoded_open.transition_epoch = round.transition_epoch + 1;
	test_decoded_open.record_generation = round.prepare_generation + 2; /* epoch mismatch */
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);
	UT_ASSERT(!test_bit22_latch_active);

	/* ACTIVE root + exact cross-match -> restored with the OPEN record's
	 * round identity (TARGET_BOOTSTRAP). */
	test_decoded_open.transition_epoch = round.transition_epoch;
	test_decoded_open.record_generation = round.prepare_generation + 2;
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 1);
	UT_ASSERT_EQ(test_bit22_latch_apply_epoch, round.transition_epoch);
	UT_ASSERT_EQ(test_bit22_latch_apply_generation, round.prepare_generation + 2);
	UT_ASSERT(test_bit22_latch_active);

	/* Already armed -> no second apply. */
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 0);

	/* Refused apply (census RED stand-in) -> fail-closed, gate stays off. */
	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_decoded_open.transition_epoch = round.transition_epoch;
	test_decoded_open.record_generation = round.prepare_generation + 2;
	test_bit22_latch_active = false;
	test_bit22_latch_apply_ok = false;
	test_bit22_latch_apply_calls = 0;
	UT_ASSERT(!cluster_control_root_restore_bit22_latch_if_active());
	UT_ASSERT_EQ(test_bit22_latch_apply_calls, 1);
	UT_ASSERT(!test_bit22_latch_active);
	test_bit22_latch_apply_ok = true;
}

UT_TEST(test_unbound_cutover_mutators_fail_before_cf_and_preserve_prepared_root)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	int lock_calls_before;
	int rename_calls_before;

	wipe_root_files();
	build_migration(&image, &round);
	memset(&prepared, 0xee, sizeof(prepared));
	test_create_authorized = false;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &prepared),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
	UT_ASSERT_EQ(prepared.file_txn_seq, 0);

	test_create_authorized = true;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &prepared),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	round_sha256(&round, round_sha);
	lock_calls_before = test_cf_lock_calls;
	rename_calls_before = test_durable_rename_calls;
	memset(&active, 0xee, sizeof(active));
	test_activate_authorized = false;
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, lock_calls_before);
	UT_ASSERT_EQ(test_durable_rename_calls, rename_calls_before);
	UT_ASSERT_EQ(active.file_txn_seq, 0);

	/* The refused attempt cannot consume or mutate the PREPARED image. */
	test_activate_authorized = true;
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(active.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_ACTIVE);
	UT_ASSERT_EQ(active.file_txn_seq, prepared.file_txn_seq + 1);
}

UT_TEST(test_native_cf_hold_cannot_authorize_strong_read)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_clusterwide = false;
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_activation_rejects_changed_source_wal_bytes)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterWalStateHeader header;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	char path[MAXPGPATH];
	int fd;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	snprintf(path, sizeof(path), "%s/%s", test_wal_root, CLUSTER_WAL_STATE_FILENAME);
	fd = open(path, O_RDWR);
	UT_ASSERT(fd >= 0);
	cluster_wal_state_header_fill(&header, INT64_C(1700000000000999));
	UT_ASSERT_EQ(pwrite(fd, &header, sizeof(header), 0), sizeof(header));
	close(fd);
	round_sha256(&round, round_sha);
	memset(&active, 0xee, sizeof(active));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(active.file_txn_seq, 0);
	build_source_wal_state();
}

UT_TEST(test_activation_rejects_same_node_thread_claim_drift)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken prepared;
	ClusterControlRootFileToken active;
	ClusterWalThreadClaim claim;
	uint8 round_sha[PG_SHA256_DIGEST_LENGTH];
	char path[MAXPGPATH];

	wipe_root_files();
	build_source_wal_state();
	UT_ASSERT_EQ(create_prepared(&image, &round, &prepared), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	cluster_wal_thread_claim_fill(&claim, 1, 0, INT64_C(1699999999000999));
	snprintf(path, sizeof(path), "%s/thread_1/%s", test_wal_root,
			 CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	write_all_or_abort(path, &claim, sizeof(claim));
	round_sha256(&round, round_sha);
	memset(&active, 0xee, sizeof(active));
	UT_ASSERT_EQ(cluster_control_root_activate_prepared(&prepared, round_sha, &round, &active),
				 CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT_EQ(active.file_txn_seq, 0);
	build_source_wal_state();
}

UT_TEST(test_forbidden_patch_rejected_before_cf_and_file_io)
{
	ClusterControlRootReadToken token;
	ClusterControlRootPatch patch;
	ClusterControlRootSnapshot snapshot;

	wipe_root_files();
	memset(&token, 0, sizeof(token));
	token.source = 1;
	token.origin_thread_id = 1;
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x04);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	test_cf_lock_calls = 0;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &snapshot, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
}

UT_TEST(test_lookup_and_revalidate_use_exact_primary_identity)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;
	ClusterControlRootReadToken lookup_token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_revalidate(&token, &image.records[0].identity, &snapshot),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &lookup_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(cluster_control_root_identity_equal(&identity, &image.records[0].identity));
	UT_ASSERT_EQ(lookup_token.file_txn_seq, token.file_txn_seq);
}

UT_TEST(test_lifecycle_publish_exact_token_cas)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &published,
					 &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED);
	UT_ASSERT_EQ(published.root_publish_seq, snapshot.root_publish_seq + 1);
	UT_ASSERT_EQ(new_token.file_txn_seq, read_token.file_txn_seq + 1);
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &read_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &published,
					 &new_token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(test_walr_begin_calls, 0);
	UT_ASSERT_EQ(test_walr_end_calls, 0);
}

UT_TEST(test_retention_expanding_publish_refuses_before_cf_without_walr)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	test_walr_begin_result = CLUSTER_WAL_PIN_UNAVAILABLE;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_thread, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_retention_expanding_publish_holds_walr_around_cf_and_readback)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	test_order_seq = 0;
	test_walr_begin_order = 0;
	test_cf_acquire_order = 0;
	test_cf_release_order = 0;
	test_last_rename_order = 0;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	UT_ASSERT(test_walr_begin_order < test_cf_acquire_order);
	UT_ASSERT(test_cf_acquire_order < test_last_rename_order);
	UT_ASSERT(test_last_rename_order < test_cf_release_order);
	UT_ASSERT(test_cf_release_order < test_order_seq);
}

UT_TEST(test_unbound_publisher_fails_before_cf_and_preserves_root)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot before;
	ClusterControlRootSnapshot after;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken before_token;
	ClusterControlRootReadToken after_token;
	ClusterControlRootReadToken published_token;
	ClusterControlRootPatch patch;
	int lock_calls_before_publish;

	wipe_root_files();
	test_publish_authorized = true;
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &before,
													 &before_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = CLUSTER_CONTROL_ROOT_PATCH_LIFECYCLE;
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
	memset(&published, 0xee, sizeof(published));
	memset(&published_token, 0xee, sizeof(published_token));
	lock_calls_before_publish = test_cf_lock_calls;
	test_publish_authorized = false;
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(
					 &before_token, &patch, CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_RETIRE, &published,
					 &published_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(test_cf_lock_calls, lock_calls_before_publish);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(published_token.file_txn_seq, 0);

	test_publish_authorized = true;
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &after,
													 &after_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(after.lifecycle, before.lifecycle);
	UT_ASSERT_EQ(after.root_publish_seq, before.root_publish_seq);
	UT_ASSERT_EQ(after_token.file_txn_seq, before_token.file_txn_seq);
}

UT_TEST(test_owner_rejoin_rejects_non_new_incarnation)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &read_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x3b);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = snapshot.identity.origin_owner_incarnation;
	patch.desired.identity.root_lineage_seq = snapshot.identity.root_lineage_seq + 1;
	patch.desired.root_flags
		= CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID | CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID;
	patch.desired.checkpoint_tli = snapshot.checkpoint_tli;
	patch.desired.checkpoint_source_kind = snapshot.checkpoint_source_kind;
	patch.desired.checkpoint_lower_lsn = snapshot.checkpoint_lower_lsn;
	patch.desired.checkpoint_record_crc32c = snapshot.checkpoint_record_crc32c;
	patch.desired.recovered_through_lsn_exclusive = snapshot.checkpoint_lower_lsn;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_owner_rejoin_advances_exact_lineage_and_exhausts_at_max)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;
	uint64 new_incarnation;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	build_owner_rejoin_patch(&snapshot, new_incarnation, snapshot.identity.root_lineage_seq + 1,
							 &patch);
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, 2);

	/* Rebuild an otherwise valid RECOVERY_COMPLETE root at the terminal
	 * lineage.  OWNER_REJOIN must fail closed; UINT64_MAX never wraps. */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	force_first_record_lineage(UINT64_MAX);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(snapshot.identity.root_lineage_seq, UINT64_MAX);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1, UINT64_C(1),
							 &patch);
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_CAS_CONFLICT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
}

UT_TEST(test_lifecycle_frozen_shape_matrix)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootIdentity identity;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootSnapshot published;
	ClusterControlRootReadToken read_token;
	ClusterControlRootReadToken new_token;
	ClusterControlRootPatch patch;
	uint64 new_incarnation;

	/* ① OWNER_REJOIN from RECOVERY_COMPLETE -> OPEN succeeds (frozen
	 * crash-rejoin mainline). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_COMPLETE;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	build_owner_rejoin_patch(&snapshot, new_incarnation, snapshot.identity.root_lineage_seq + 1,
							 &patch);
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, snapshot.identity.root_lineage_seq + 1);

	/* ② OWNER_REJOIN from OPEN is rejected by patch_shape_valid BEFORE any
	 * CF / file I/O (STOP-02 §17.4: pre-lifecycle must be
	 * RECOVERY_COMPLETE). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);

	/* ②' OWNER_REJOIN from CLOSED is rejected the same way: the
	 * clean-reopen mainline is THREAD_OPEN (CLOSED -> OPEN), never the
	 * OWNER_REJOIN CAS (increment-13 allowance removed). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	build_owner_rejoin_patch(&snapshot, snapshot.identity.origin_owner_incarnation + 1,
							 snapshot.identity.root_lineage_seq + 1, &patch);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	test_cf_lock_calls = 0;
	test_durable_rename_calls = 0;
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_OWNER_REJOIN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(published.identity.system_identifier, 0);
	UT_ASSERT_EQ(new_token.file_txn_seq, 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	UT_ASSERT_EQ(test_durable_rename_calls, 0);

	/* ③ THREAD_OPEN CLOSED -> OPEN succeeds with owner re-stamp +
	 * lineage+1 (the frozen clean-reopen mainline). */
	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &file_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(
		cluster_control_root_lookup_owner_by_node_runtime(0, &identity, &snapshot, &read_token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	new_incarnation = snapshot.identity.origin_owner_incarnation + 1;
	memset(&patch, 0, sizeof(patch));
	patch.mask = UINT64_C(0x3b);
	patch.expected_lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
	patch.desired.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	patch.desired.identity.origin_owner_incarnation = new_incarnation;
	patch.desired.identity.root_lineage_seq = snapshot.identity.root_lineage_seq + 1;
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
	memset(&published, 0xee, sizeof(published));
	memset(&new_token, 0xee, sizeof(new_token));
	UT_ASSERT_EQ(cluster_control_root_compare_and_publish(&read_token, &patch,
														  CLUSTER_CONTROL_ROOT_PUBLISH_THREAD_OPEN,
														  &published, &new_token),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(published.lifecycle, CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN);
	UT_ASSERT_EQ(published.identity.origin_owner_incarnation, new_incarnation);
	UT_ASSERT_EQ(published.identity.root_lineage_seq, snapshot.identity.root_lineage_seq + 1);
}

UT_TEST(test_initial_migration_requires_lineage_one)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;

	wipe_root_files();
	build_migration(&image, &round);
	image.records[0].identity.root_lineage_seq = 2;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_create_prepared(&image, &round, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_unconfirmed_release_returns_no_authority)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken file_token;
	ClusterControlRootSnapshot snapshot;
	ClusterControlRootReadToken token;

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &file_token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	test_cf_release_confirmed = false;
	memset(&snapshot, 0xee, sizeof(snapshot));
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 &token),
				 CLUSTER_CONTROL_ROOT_RELEASE_UNCERTAIN);
	UT_ASSERT_EQ(snapshot.identity.system_identifier, 0);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
}

UT_TEST(test_primary_rename_failure_is_not_success)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	struct stat st;

	wipe_root_files();
	test_fail_primary_rename = true;
	memset(&token, 0xee, sizeof(token));
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT_EQ(token.file_txn_seq, 0);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	UT_ASSERT(lstat(path, &st) != 0 && errno == ENOENT);
}

UT_TEST(test_reserved_bytes_and_symlink_fail_closed)
{
	ClusterControlRootMigrationImage image;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	uint8 bytes[CLUSTER_CONTROL_ROOT_FILE_BYTES];
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	bytes[196] = 1;
	put_u32_le(bytes + 504, image_crc(bytes, 504));
	write_all_or_abort(primary, bytes, sizeof(bytes));
	unlink(bak);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &image.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 NULL),
				 CLUSTER_CONTROL_ROOT_BAD_RESERVED);

	wipe_root_files();
	UT_ASSERT_EQ(symlink("/tmp/foreign-pgrac-root", primary), 0);
	UT_ASSERT_EQ(create_prepared(&image, &round, &token), CLUSTER_CONTROL_ROOT_IO_ERROR);
}

int
main(int argc, char **argv)
{
	if (argc > 1 && strcmp(argv[1], "--fixture-root-cast") == 0)
		return fixture_cast_main(argc, argv);
	if (argc > 1)
		return fixture_root_main(argc, argv);
	setup_fixture();

	UT_PLAN(34);
	UT_RUN(test_abi_identity_and_features);
	UT_RUN(test_invalid_argument_precedes_authority_io);
	UT_RUN(test_external_fence_bit24_activation_is_forbidden_without_provider);
	UT_RUN(test_create_and_read_primary);
	UT_RUN(test_bootstrap_read_never_returns_authority_token);
	UT_RUN(test_round_sha256_is_deterministic_and_matches_create);
	UT_RUN(test_build_migration_image_maps_registry_and_claims);
	UT_RUN(test_build_migration_image_accepts_frozen_active_slot);
	UT_RUN(test_build_migration_image_rejects_non_stopped_slot);
	UT_RUN(test_strong_read_null_identity_stays_invalid_argument);
	UT_RUN(test_discovered_read_binds_identity_and_mints_token);
	UT_RUN(test_discovered_read_absent_thread_fails_closed);
	UT_RUN(test_valid_bak_blocks_corrupt_primary);
	UT_RUN(test_storage_contract_fails_before_cf_or_file_io);
	UT_RUN(test_single_node_local_probe_fails_before_cf_or_file_io);
	UT_RUN(test_activate_and_stale_token);
	UT_RUN(test_restore_bit22_latch_from_active_root);
	UT_RUN(test_unbound_cutover_mutators_fail_before_cf_and_preserve_prepared_root);
	UT_RUN(test_native_cf_hold_cannot_authorize_strong_read);
	UT_RUN(test_activation_rejects_changed_source_wal_bytes);
	UT_RUN(test_activation_rejects_same_node_thread_claim_drift);
	UT_RUN(test_forbidden_patch_rejected_before_cf_and_file_io);
	UT_RUN(test_lookup_and_revalidate_use_exact_primary_identity);
	UT_RUN(test_lifecycle_publish_exact_token_cas);
	UT_RUN(test_retention_expanding_publish_refuses_before_cf_without_walr);
	UT_RUN(test_retention_expanding_publish_holds_walr_around_cf_and_readback);
	UT_RUN(test_unbound_publisher_fails_before_cf_and_preserves_root);
	UT_RUN(test_owner_rejoin_rejects_non_new_incarnation);
	UT_RUN(test_owner_rejoin_advances_exact_lineage_and_exhausts_at_max);
	UT_RUN(test_lifecycle_frozen_shape_matrix);
	UT_RUN(test_initial_migration_requires_lineage_one);
	UT_RUN(test_unconfirmed_release_returns_no_authority);
	UT_RUN(test_primary_rename_failure_is_not_success);
	UT_RUN(test_reserved_bytes_and_symlink_fail_closed);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
