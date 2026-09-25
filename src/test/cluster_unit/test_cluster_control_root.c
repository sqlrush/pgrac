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
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_shared_config.h"
#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_cf_stats.h"
#include "cluster/cluster_cf_storage.h"
#include "cluster/cluster_control_root.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_reconfig.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_stats.h"
#include "cluster/cluster_wal_retention.h"
#include "cluster/cluster_wal_state.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/storage/cluster_shared_fs.h"
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/timestamp.h"

#include "../../backend/cluster/cluster_control_root_private.h"
#include "../../backend/cluster/cluster_control_bootstrap_private.h"
#include "../../backend/cluster/cluster_recovery_anchor_private.h"

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
bool enableFsync = true;
bool cluster_enabled = true;
bool cluster_controlfile_shared_authority = true;
bool cluster_shared_config = false;

/* PGRAC: facts, not an authorization stub; real publisher performs checks.
 * Author: SqlRush <sqlrush@gmail.com>
 */
AuxProcType MyAuxProcType = NotAnAuxProcess;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static bool test_checkpoint_mode;
static uint64 test_self_incarnation;
static uint64 test_epoch;
static unsigned test_epoch_reads, test_change_epoch_read;
static bool test_serving, test_fence, test_prebump, test_wal_validated;
static ClusterMembershipState test_member_state;
static XLogRecPtr test_flush;
static TimeLineID test_flush_tli;
static void (*test_checkpoint_x_hook)(void);
static bool test_fence_after_primary, test_release_after_primary;
static bool test_checkpoint_outer_cf;

void
pg_re_throw(void)
{
	if (PG_exception_stack != NULL)
		siglongjmp(*PG_exception_stack, 1);
	abort();
}

uint64
cluster_epoch_get_current(void)
{
	if (!test_checkpoint_mode)
		abort();
	if (++test_epoch_reads == test_change_epoch_read)
		++test_epoch;
	return test_epoch;
}

bool
cluster_serving_ready_is_current(void)
{
	if (!test_checkpoint_mode)
		abort();
	return test_serving;
}

bool
cluster_reconfig_has_pending_prebump_stage(void)
{
	if (!test_checkpoint_mode)
		abort();
	return test_prebump;
}

XLogRecPtr
GetFlushRecPtr(TimeLineID *tli)
{
	if (!test_checkpoint_mode)
		abort();
	if (tli != NULL)
		*tli = test_flush_tli;
	return test_flush;
}

/* PGRAC: linked legacy anchor writer dependencies must not be used by this
 * read-only v2 integration. Abort if a new path accidentally calls them.
 * Author: SqlRush <sqlrush@gmail.com>
 */
ClusterMembershipState
cluster_membership_get_state(int32 node)
{
	(void)node;
	if (test_checkpoint_mode)
		return test_member_state;
	abort();
}
ClusterStartupPhase
cluster_current_phase(void)
{
	abort();
}
TimestampTz
cluster_phase_started_at(ClusterStartupPhase phase)
{
	(void)phase;
	abort();
}
ClusterStatsStatus
cluster_stats_status(void)
{
	abort();
}
TimestampTz
cluster_stats_spawned_at(void)
{
	abort();
}
uint64
cluster_qvotec_get_self_incarnation(void)
{
	if (test_checkpoint_mode)
		return test_self_incarnation;
	abort();
}
uint16
cluster_wal_thread_dump_thread_id(void)
{
	abort();
}
bool
cluster_wal_thread_dir_configured(void)
{
	abort();
}
bool
cluster_wal_thread_dir_validated(void)
{
	if (test_checkpoint_mode)
		return test_wal_validated;
	abort();
}
bool
cluster_wal_state_registry_ready(void)
{
	abort();
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread, ClusterWalStateSlot *slot)
{
	(void)thread;
	(void)slot;
	abort();
}
bool
cluster_cf_exactly_one_declared_node(void)
{
	abort();
}
bool
cluster_cf_held(LOCKMODE mode)
{
	(void)mode;
	if (test_checkpoint_mode)
		return test_checkpoint_outer_cf;
	abort();
}
bool
cluster_cf_owner_eor_local_active(void)
{
	abort();
}
bool
cluster_write_fence_allowed(void)
{
	if (test_checkpoint_mode)
		return test_fence;
	abort();
}
void
cluster_write_fence_reject_if_fenced(const char *op)
{
	(void)op;
	abort();
}

static char test_root[MAXPGPATH];
static char test_wal_root[MAXPGPATH];
static uint64 test_system_identifier = TEST_SYSID;
static ControlFileData *test_sysid_control;
static char test_storage_uuid_text[33] = "00112233445566778899aabbccddeeff";
static ClusterCfContractState test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
static int test_node_count = 4;
static bool test_local_probe = true;
static bool test_cf_grant = true;
static bool test_cf_clusterwide = true;
static LOCKMODE test_cf_mode = NoLock;
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

/* PGRAC: legacy native-control error/fallback symbols are linked but must
 * never select a fallback in the v2 view tests. Author: SqlRush <sqlrush@gmail.com>
 */
int
errcode_for_file_access(void)
{
	return 0;
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
	return false;
}

bool
cluster_cf_bak_checkpoint_recoverable(const ControlFileData *bak pg_attribute_unused())
{
	return false;
}

void
cluster_cf_counter_inc(ClusterCfCounter which pg_attribute_unused())
{}

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
	if (strstr(newfile, CLUSTER_CONTROL_ROOT_REL_PATH) != NULL && strstr(newfile, ".bak") == NULL) {
		if (test_fence_after_primary)
			test_fence = false;
		if (test_release_after_primary)
			test_cf_release_confirmed = false;
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
	return test_sysid_control != NULL ? test_sysid_control->system_identifier
									  : test_system_identifier;
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
	if (mode == ExclusiveLock && test_checkpoint_x_hook != NULL)
		test_checkpoint_x_hook();
	return test_cf_grant;
}

bool
cluster_cf_held_is_clusterwide(LOCKMODE mode)
{
	return test_cf_grant && test_cf_clusterwide && (test_cf_mode == NoLock || test_cf_mode == mode);
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
	test_cf_mode = NoLock;
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
	test_checkpoint_mode = false;
	test_checkpoint_x_hook = NULL;
	test_fence_after_primary = test_release_after_primary = false;
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

/* PGRAC: root-v2 fixtures are independently assembled bytes.  In particular,
 * no production encoder computes the expected image or its field offsets.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static const uint8 v2_storage[16] = { 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77,
									  0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff };

static bool
v2_zero(const void *ptr, size_t len)
{
	const uint8 *p = ptr;
	size_t i;

	for (i = 0; i < len; ++i)
		if (p[i] != 0)
			return false;
	return true;
}

static void
v2_root_checksums(uint8 *bytes)
{
	put_u32_le(bytes + 96, image_crc(bytes + 512, 65536));
	put_u32_le(bytes + 504, image_crc(bytes, 504));
}

static void
v2_checksums(uint8 *bytes)
{
	int i;

	for (i = 0; i < 128; ++i) {
		uint8 *record = bytes + 512 + 512 * i;

		if (!v2_zero(record, 512))
			put_u32_le(record + 504, image_crc(record, 504));
	}
	v2_root_checksums(bytes);
}

static void
v2_fixture(uint8 bytes[66048])
{
	int node;

	memset(bytes, 0, 66048);
	memcpy(bytes, "PGCH", 4);
	put_u16_le(bytes + 4, 2);
	put_u16_le(bytes + 6, 512);
	put_u16_le(bytes + 8, 512);
	put_u16_le(bytes + 10, 128);
	put_u32_le(bytes + 12, UINT32_C(0x01020304));
	put_u64_le(bytes + 16, 7);
	put_u64_le(bytes + 24, TEST_SYSID);
	memcpy(bytes + 32, v2_storage, 16);
	memset(bytes + 48, 0xab, 16);
	bytes[54] = 0x4b;
	bytes[56] = 0x8b;
	put_u64_le(bytes + 64, 0x0d);
	put_u16_le(bytes + 72, 2);
	put_u16_le(bytes + 74, 2);
	put_u32_le(bytes + 76, 2);
	put_u64_le(bytes + 80, 111);
	put_u64_le(bytes + 88, 222);
	memset(bytes + 100, 0x11, 32);
	memset(bytes + 132, 0x22, 32);
	put_u64_le(bytes + 164, 3);
	put_u64_le(bytes + 172, 4);
	put_u64_le(bytes + 180, 1);
	put_u64_le(bytes + 188, UINT64_C(0x400001));
	put_u32_le(bytes + 196, 3);
	put_u64_le(bytes + 200, 41);
	put_u64_le(bytes + 208, 43);
	put_u64_le(bytes + 216, 1);
	put_u64_le(bytes + 224, UINT64_C(1) << 63);
	put_u64_le(bytes + 232, 1);
	put_u64_le(bytes + 240, UINT64_C(1) << 63);
	put_u64_le(bytes + 248, 47);
	memset(bytes + 256, 0x33, 32);
	put_u64_le(bytes + 288, 53);
	memset(bytes + 296, 0x44, 32);
	put_u64_le(bytes + 328, 59);
	put_u64_le(bytes + 336, 61);
	memset(bytes + 344, 0x55, 32);
	for (node = 0; node <= 127; node += 127) {
		uint8 *r = bytes + 512 + node * 512;

		memcpy(r, "PGRT", 4);
		put_u16_le(r + 4, 2);
		put_u16_le(r + 6, 512);
		put_u16_le(r + 8, node + 1);
		r[10] = 1;
		put_u32_le(r + 12, node);
		put_u64_le(r + 16, 10 + node);
		put_u64_le(r + 24, 11 + node);
		put_u64_le(r + 32, TEST_SYSID);
		memcpy(r + 40, bytes + 32, 32);
		put_u64_le(r + 72, 12345 + node);
		put_u64_le(r + 80, 99 + node);
		put_u32_le(r + 96, 1);
		put_u32_le(r + 108, 5);
		put_u64_le(r + 112, UINT64_C(0x1000000) + node * 4096);
		put_u64_le(r + 144, 777);
		put_u32_le(r + 152, 2);
		put_u32_le(r + 156, 11);
		put_u64_le(r + 160, 333);
		put_u32_le(r + 168, 1233 + node);
		put_u32_le(r + 172, 1234 + node);
		put_u16_le(r + 194, 2);
		if (node == 127) {
			put_u64_le(r + 216, 44);
			memset(r + 224, 0x66, 32);
		}
		put_u64_le(r + 256, 66 + node);
		memset(r + 264, 0x77, 32);
		memset(r + 296, 0x88, 32);
	}
	v2_checksums(bytes);
}

/* PGRAC: literal retained-input bytes, not a codec-generated expectation.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static size_t
history_fixture(uint8 bytes[65604], ControlRootImage *root, uint32 node, uint32 count)
{
	uint8 raw_root[66048];
	size_t length = 64 + count * 512 + 4;

	v2_fixture(raw_root);
	if (cluster_control_root_v2_decode(raw_root, sizeof(raw_root), v2_storage, TEST_SYSID, root))
		abort();
	memset(bytes, 0, 65604);
	memcpy(bytes, "PGWH", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 64);
	put_u32_le(bytes + 8, count);
	put_u32_le(bytes + 12, 512);
	put_u64_le(bytes + 16, count * 512);
	put_u64_le(bytes + 24, TEST_SYSID);
	memcpy(bytes + 32, v2_storage, 16);
	memcpy(bytes + 48, raw_root + 48, 16);
	for (uint32 i = 0; i < count; i++) {
		uint8 *record = bytes + 64 + i * 512;
		memcpy(record, raw_root + 512 + node * 512, 512);
		/* Deliberately unlike the current writer; no numeric age inference. */
		put_u64_le(record + 80, 1000 + i);
		put_u64_le(record + 24, 2000 + i);
		put_u64_le(record + 72, 3000 + i);
		put_u32_le(record + 168, 4000 + i);
		record[10] = 1 + i % 5;
		memset(record + 216, 0, 40);
		put_u32_le(record + 504, image_crc(record, 504));
	}
	put_u32_le(bytes + length - 4, image_crc(bytes, length - 4));
	root->refs[node].history_generation = 123;
	sha256_bytes(bytes, length, root->refs[node].history_sha256);
	return length;
}

static void
history_outer_checksum(uint8 *bytes, size_t len, ControlRootImage *root, uint32 node)
{
	put_u32_le(bytes + len - 4, image_crc(bytes, len - 4));
	sha256_bytes(bytes, len, root->refs[node].history_sha256);
}

static ClusterControlRootResult
history_refused(const uint8 *bytes, size_t len, const ControlRootImage *root, uint32 node)
{
	ClusterWalHistoryImage out;
	ClusterControlRootResult result;
	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_root_v2_history_decode(bytes, len, root, node, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	return result;
}

UT_TEST(test_history_exact_empty_and_full_set_preserves_inputs)
{
	uint8 bytes[65604];
	ControlRootImage root;
	ClusterWalHistoryImage out;
	const uint32 counts[] = { 0, 2, 128 };
	for (uint32 node = 0; node <= 127; node += 127)
		for (size_t c = 0; c < lengthof(counts); c++) {
			uint8 hash[32];
			size_t len = history_fixture(bytes, &root, node, counts[c]);
			ClusterControlRootResult result
				= cluster_control_root_v2_history_decode(bytes, len, &root, node, &out);
			UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
			if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
				continue;
			UT_ASSERT_EQ(out.count, counts[c]);
			for (uint32 i = 0; i < out.count; i++) {
				const ClusterWalHistoryRecord *record = &out.records[i];
				UT_ASSERT_EQ(record->snapshot.identity.origin_node_id, node);
				UT_ASSERT_EQ(record->snapshot.identity.origin_thread_id, node + 1);
				UT_ASSERT_EQ(record->snapshot.identity.origin_owner_incarnation, 1000 + i);
				UT_ASSERT_EQ(record->snapshot.identity.root_lineage_seq, 2000 + i);
				UT_ASSERT_EQ(record->snapshot.identity.thread_claim_created_at, 3000 + i);
				UT_ASSERT_EQ(record->snapshot.identity.thread_claim_crc32c, 4000 + i);
				UT_ASSERT_EQ(record->snapshot.lifecycle, 1 + i % 5);
				UT_ASSERT_EQ(record->snapshot.checkpoint_lower_lsn,
							 UINT64_C(0x1000000) + node * 4096);
				UT_ASSERT_EQ(record->publisher_incarnation, 777);
				UT_ASSERT_EQ(record->publisher_node, 2);
				UT_ASSERT_EQ(record->refs.anchor_generation, 66 + node);
				UT_ASSERT_EQ(record->refs.history_generation, 0);
				UT_ASSERT(memcmp(record->refs.anchor_sha256, bytes + 64 + i * 512 + 264, 32) == 0);
				UT_ASSERT(memcmp(record->refs.claim_sha256, bytes + 64 + i * 512 + 296, 32) == 0);
				UT_ASSERT_EQ(record->record_crc32c, image_crc(bytes + 64 + i * 512, 504));
			}
			UT_ASSERT(v2_zero(&out.records[counts[c]], (128 - counts[c]) * sizeof(out.records[0])));
			/* Exact hash remains bound; decoder does not canonicalize inputs. */
			sha256_bytes(bytes, len, hash);
			UT_ASSERT(memcmp(hash, root.refs[node].history_sha256, 32) == 0);
		}
}

UT_TEST(test_history_rejects_outer_shape_crc_hash_and_identity)
{
	uint8 bytes[65604];
	ControlRootImage root;
	size_t len;
	for (int fault = 0; fault < 13; fault++) {
		size_t case_len = history_fixture(bytes, &root, 0, 2);
		ClusterControlRootResult expected = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		switch (fault) {
		case 0:
			bytes[0] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_BAD_MAGIC;
			break;
		case 1:
			put_u16_le(bytes + 4, 2);
			expected = CLUSTER_CONTROL_ROOT_BAD_VERSION;
			break;
		case 2:
			put_u16_le(bytes + 6, 63);
			break;
		case 3:
			put_u32_le(bytes + 8, 129);
			break;
		case 4:
			put_u32_le(bytes + 8, UINT32_MAX);
			break;
		case 5:
			put_u32_le(bytes + 12, 511);
			break;
		case 6:
			put_u64_le(bytes + 16, 1023);
			break;
		case 7:
			bytes[24] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 8:
			bytes[32] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 9:
			bytes[48] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 10:
			case_len--;
			break;
		case 11:
			case_len++;
			break;
		case 12:
			expected = CLUSTER_CONTROL_ROOT_BAD_BODY_CRC;
			break;
		}
		history_outer_checksum(bytes, case_len, &root, 0);
		if (fault == 12) {
			bytes[case_len - 1] ^= 1;
			sha256_bytes(bytes, case_len, root.refs[0].history_sha256);
		}
		UT_ASSERT_EQ(history_refused(bytes, case_len, &root, 0), expected);
	}
	len = history_fixture(bytes, &root, 0, 2);
	root.refs[0].history_sha256[0] ^= 1;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
}

UT_TEST(test_history_rejects_bad_records_and_namespace_aliases)
{
	uint8 bytes[65604];
	ControlRootImage root;
	for (int fault = 0; fault < 14; fault++) {
		size_t len = history_fixture(bytes, &root, 0, 2);
		uint8 *record = bytes + 64 + 512;
		ClusterControlRootResult expected = CLUSTER_CONTROL_ROOT_RANGE_INVALID;
		switch (fault) {
		case 0:
			record[0] ^= 1;
			expected = CLUSTER_CONTROL_ROOT_BAD_MAGIC;
			break;
		case 1:
			put_u16_le(record + 4, 1);
			expected = CLUSTER_CONTROL_ROOT_BAD_VERSION;
			break;
		case 2:
			record[328] = 1;
			expected = CLUSTER_CONTROL_ROOT_BAD_RESERVED;
			break;
		case 3:
			put_u16_le(record + 8, 2);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 4:
			put_u32_le(record + 12, 1);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 5:
			put_u64_le(record + 32, TEST_SYSID + 1);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 6:
			put_u64_le(record + 80, 1000);
			break; /* Different claim, same namespace. */
		case 7:
			put_u64_le(record + 80, 999);
			break; /* Not canonical order. */
		case 8:
			put_u64_le(record + 80, 99);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
			break;
		case 9:
			put_u64_le(record + 216, 45);
			memset(record + 224, 1, 32);
			break;
		case 10:
			put_u64_le(record + 144, 0);
			expected = CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID;
			break;
		case 11:
			put_u64_le(record + 112, 0);
			break;
		case 12:
			expected = CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
			break;
		case 13:
			memset(record, 0, 512);
			break;
		}
		if (fault != 13)
			put_u32_le(record + 504, image_crc(record, 504));
		if (fault == 12)
			record[504] ^= 1;
		history_outer_checksum(bytes, len, &root, 0);
		UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), expected);
	}
}

UT_TEST(test_history_refuses_unselected_and_invalid_arguments)
{
	uint8 bytes[65604];
	ControlRootImage root;
	size_t len = history_fixture(bytes, &root, 0, 2);
	const size_t sizes[] = { 0, 4, 63, 67, 65605, SIZE_MAX };
	UT_ASSERT_EQ(history_refused(NULL, len, &root, 0), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(history_refused(bytes, len, NULL, 0), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 128), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &root, 0, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (size_t i = 0; i < lengthof(sizes); i++)
		UT_ASSERT_EQ(history_refused(bytes, sizes[i], &root, 0), CLUSTER_CONTROL_ROOT_BAD_SIZE);
	root.present[0] = false;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_ABSENT);
	root.present[0] = true;
	root.refs[0].history_generation = 0;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
	memset(root.refs[0].history_sha256, 0, 32);
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_ABSENT);
	len = history_fixture(bytes, &root, 0, 2);
	root.header.format_version = 1;
	UT_ASSERT_EQ(history_refused(bytes, len, &root, 0), CLUSTER_CONTROL_ROOT_BAD_VERSION);
}

UT_TEST(test_history_refuses_alias_before_clearing_output)
{
	union {
		uint8 bytes[65604];
		ControlRootImage root;
		ClusterWalHistoryImage out;
	} storage;
	uint8 bytes[65604];
	ControlRootImage root;
	size_t len = history_fixture(storage.bytes, &root, 0, 2);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(storage.bytes, len, &root, 0, &storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
	len = history_fixture(bytes, &storage.root, 0, 2);
	UT_ASSERT_EQ(cluster_control_root_v2_history_decode(bytes, len, &storage.root, 0, &storage.out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&storage.out, sizeof(storage.out)));
}

UT_TEST(test_v2_decodes_exact_common_and_two_thread_fields)
{
	uint8 bytes[66048];
	ControlRootImage out;
	int node;

	v2_fixture(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.header.format_version, 2);
	UT_ASSERT_EQ(out.header.file_txn_seq, 7);
	UT_ASSERT_EQ(out.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
	UT_ASSERT_EQ(out.header.v2.database_incarnation, 41);
	UT_ASSERT_EQ(out.header.v2.formation_seq, 43);
	UT_ASSERT_EQ(out.header.v2.configured[0], 1);
	UT_ASSERT_EQ(out.header.v2.configured[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(out.header.v2.serving[0], 1);
	UT_ASSERT_EQ(out.header.v2.serving[1], UINT64_C(1) << 63);
	UT_ASSERT_EQ(out.header.v2.config_generation, 47);
	UT_ASSERT_EQ(out.header.v2.control_image_generation, 53);
	UT_ASSERT_EQ(out.header.v2.catalog_manifest_generation, 59);
	UT_ASSERT_EQ(out.header.v2.global_scn_high_water, 61);
	UT_ASSERT(memcmp(out.header.v2.config_sha256, bytes + 256, 32) == 0);
	UT_ASSERT(memcmp(out.header.v2.control_image_sha256, bytes + 296, 32) == 0);
	UT_ASSERT(memcmp(out.header.v2.catalog_manifest_sha256, bytes + 344, 32) == 0);
	for (node = 0; node <= 127; node += 127) {
		UT_ASSERT(out.present[node]);
		UT_ASSERT_EQ(out.records[node].identity.origin_thread_id, node + 1);
		UT_ASSERT_EQ(out.records[node].identity.origin_node_id, node);
		UT_ASSERT_EQ(out.records[node].identity.origin_owner_incarnation, 99 + node);
		UT_ASSERT_EQ(out.records[node].checkpoint_lower_lsn, UINT64_C(0x1000000) + node * 4096);
		UT_ASSERT_EQ(out.publisher_incarnation[node], 777);
		UT_ASSERT_EQ(out.publisher_node[node], 2);
		UT_ASSERT_EQ(out.refs[node].anchor_generation, 66 + node);
		UT_ASSERT(memcmp(out.refs[node].anchor_sha256, bytes + 512 + node * 512 + 264, 32) == 0);
		UT_ASSERT(memcmp(out.refs[node].claim_sha256, bytes + 512 + node * 512 + 296, 32) == 0);
	}
	UT_ASSERT_EQ(out.refs[127].history_generation, 44);
	UT_ASSERT(memcmp(out.refs[127].history_sha256, bytes + 512 + 127 * 512 + 224, 32) == 0);
	UT_ASSERT(v2_zero(&out.refs[0].history_sha256, 32));
	UT_ASSERT_EQ(out.refs[0].history_generation, 0);
	UT_ASSERT(!out.present[1] && v2_zero(&out.records[1], sizeof(out.records[1])));
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v2_encoder_preserves_exact_bytes_and_publishers)
{
	uint8 bytes[66048];
	ControlRootImage out;

	v2_fixture(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	memset(out.bytes, 0xee, sizeof(out.bytes));
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
	out.header.v2.config_generation = 48;
	put_u64_le(bytes + 248, 48);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v2_versions_are_independent_and_strict)
{
	static const size_t offsets[] = { 4, 72, 74, 512 + 4, 512 + 127 * 512 + 4 };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;
	uint16 version;

	for (i = 0; i < lengthof(offsets); ++i)
		for (version = 1; version <= 3; version += 2) {
			v2_fixture(bytes);
			put_u16_le(bytes + offsets[i], version);
			v2_checksums(bytes);
			memset(&out, 0xee, sizeof(out));
			UT_ASSERT_EQ(
				cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				CLUSTER_CONTROL_ROOT_BAD_VERSION);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
}

UT_TEST(test_v2_checks_all_three_crc_layers)
{
	uint8 bytes[66048];
	ControlRootImage out;
	int i;
	const ClusterControlRootResult expected[]
		= { CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC, CLUSTER_CONTROL_ROOT_BAD_BODY_CRC,
			CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC };

	for (i = 0; i < 3; ++i) {
		v2_fixture(bytes);
		bytes[i == 0 ? 196 : 512 + 216] ^= 1;
		if (i == 2)
			v2_root_checksums(bytes);
		memset(&out, 0xee, sizeof(out));
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			expected[i]);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_reserved_bytes_and_partial_holes_are_rejected)
{
	static const size_t offsets[]
		= { 376,	   503,		  508,		 511,		512 + 11,  512 + 88, 512 + 95,
			512 + 198, 512 + 207, 512 + 328, 512 + 503, 512 + 508, 512 + 511 };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;

	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes);
		bytes[offsets[i]] = 1;
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_BAD_RESERVED);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	v2_fixture(bytes);
	bytes[1024 + 296] = 1;
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_BAD_MAGIC);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_v2_requires_object_references_and_bound_membership)
{
	static const struct {
		size_t offset;
		size_t len;
	} clear[] = { { 200, 8 },
				  { 208, 8 },
				  { 248, 8 },
				  { 256, 32 },
				  { 288, 8 },
				  { 296, 32 },
				  { 328, 8 },
				  { 336, 8 },
				  { 344, 32 },
				  { 512 + 256, 8 },
				  { 512 + 264, 32 },
				  { 512 + 296, 32 },
				  { 512 + 127 * 512 + 216, 8 },
				  { 512 + 127 * 512 + 224, 32 },
				  { 224, 8 },
				  { 216, 8 } };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;

	for (i = 0; i < lengthof(clear); ++i) {
		v2_fixture(bytes);
		memset(bytes + clear[i].offset, 0, clear[i].len);
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	v2_fixture(bytes);
	memset(bytes + 216, 0, 32);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(test_v2_identity_and_node_thread_cannot_be_substituted)
{
	static const size_t offsets[]
		= { 24, 32, 48, 512 + 32, 512 + 40, 512 + 56, 512 + 8, 512 + 12, 512 + 127 * 512 + 12 };
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;

	for (i = 0; i < lengthof(offsets); ++i) {
		v2_fixture(bytes);
		if (offsets[i] == 48)
			memset(bytes + 48, 0, 16);
		else
			bytes[offsets[i]] ^= 1;
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_database_state_validation_is_not_an_open_decision)
{
	uint8 bytes[66048];
	ControlRootImage out;
	uint32 state;

	for (state = 0; state <= 7; ++state) {
		v2_fixture(bytes);
		put_u32_le(bytes + 196, state);
		v2_checksums(bytes);
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			state >= 1 && state <= 6 ? CLUSTER_CONTROL_ROOT_OK_PRIMARY
									 : CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
		if (state >= 1 && state <= 6)
			UT_ASSERT_EQ(out.header.v2.database_state, state);
		else
			UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
}

UT_TEST(test_v2_bad_arguments_and_size_clear_output)
{
	uint8 bytes[66048];
	ControlRootImage out;
	size_t i;
	static const size_t sizes[] = { 0, 512, 66047, 66049 };

	v2_fixture(bytes);
	for (i = 0; i < lengthof(sizes); ++i) {
		memset(&out, 0xee, sizeof(out));
		UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizes[i], v2_storage, TEST_SYSID, &out),
					 CLUSTER_CONTROL_ROOT_BAD_SIZE);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
	}
	UT_ASSERT_EQ(cluster_control_root_v2_decode(NULL, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), NULL, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, 0, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
}

UT_TEST(test_v2_encoder_refuses_bad_logical_fields_without_bytes)
{
	uint8 bytes[66048];
	ControlRootImage out;
	int mutation;

	v2_fixture(bytes);
	for (mutation = 0; mutation < 11; ++mutation) {
		UT_ASSERT_EQ(
			cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
			CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		switch (mutation) {
		case 0:
			out.header.v2.reserved4 = 1;
			break;
		case 1:
			out.header.v2.config_generation = 0;
			break;
		case 2:
			out.publisher_incarnation[127] = 0;
			break;
		case 3:
			out.publisher_node[127] = 128;
			break;
		case 4:
			out.records[127].identity.origin_node_id = 126;
			break;
		case 5:
			out.records[127].reserved208 = 1;
			break;
		case 6:
			out.refs[127].history_generation = 0;
			break;
		case 7:
			out.header.format_version = 1;
			break;
		case 8:
			out.refs[1].anchor_generation = 1;
			break;
		case 9:
			out.present[127] = false;
			break;
		case 10:
			memset(out.header.storage_uuid, 0, 16);
			memset(out.records[0].identity.storage_uuid, 0, 16);
			memset(out.records[127].identity.storage_uuid, 0, 16);
			break;
		}
		UT_ASSERT(cluster_control_root_v2_encode(&out) != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(out.bytes, sizeof(out.bytes)));
	}
}

UT_TEST(test_v2_max_generations_remain_readable_without_advancement)
{
	uint8 bytes[66048];
	ControlRootImage out;
	static const size_t offsets[]
		= { 16, 200, 208, 248, 288, 328, 512 + 256, 512 + 127 * 512 + 216 };
	size_t i;

	v2_fixture(bytes);
	for (i = 0; i < lengthof(offsets); ++i)
		put_u64_le(bytes + offsets[i], UINT64_MAX);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.header.v2.config_generation, UINT64_MAX);
	UT_ASSERT_EQ(cluster_control_root_v2_encode(&out), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(memcmp(bytes, out.bytes, sizeof(bytes)) == 0);
}

UT_TEST(test_v1_io_does_not_silently_consume_or_convert_v2)
{
	uint8 bytes[66048];
	ControlRootImage out;
	ClusterControlRootMigrationImage legacy;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	ClusterControlRootSnapshot snapshot;
	char primary[MAXPGPATH];
	char bak[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&legacy, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &out),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	v2_fixture(bytes);
	write_all_or_abort(primary, bytes, sizeof(bytes));
	unlink(bak);
	UT_ASSERT_EQ(cluster_control_root_read_canonical(1, &legacy.records[0].identity,
													 CLUSTER_CONTROL_ROOT_READ_STRONG, &snapshot,
													 NULL),
				 CLUSTER_CONTROL_ROOT_BAD_VERSION);
	read_all_or_abort(primary, out.bytes, sizeof(out.bytes));
	UT_ASSERT(memcmp(out.bytes, bytes, sizeof(bytes)) == 0);
}

/* PGRAC: real root/immutable-object integration; only CF/storage facts are
 * controlled. The root fixture is independently encoded, not published by a
 * bypass of a production authority guard. Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_install_native(ControlFileData *native, ClusterCfImageStage *stage)
{
	static uint8 operation = 1;
	uint8 uuid[16] = { 0 };
	char dir[MAXPGPATH];

	path_for(dir, sizeof(dir), "global/control_images");
	if (mkdir(dir, 0700) != 0 && errno != EEXIST)
		abort();
	path_for(dir, sizeof(dir), "global/control_images/.staging");
	if (mkdir(dir, 0700) != 0 && errno != EEXIST)
		abort();
	uuid[0] = operation++;
	uuid[6] = 0x42;
	uuid[8] = 0x82;
	if (cluster_cf_control_image_prepare(native, 53, uuid, stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_cf_control_image_install(stage) != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		abort();
}

/* PGRAC: root configuration references name real canonical immutable files,
 * never a fake validator returning success. Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_install_config(uint8 bytes[66048], bool foreign)
{
	ControlRootImage decoded;
	ClusterSharedConfigIdentity id;
	ClusterSharedConfigEntry entry = { -1, "cluster.enabled", "on" };
	char config[1024], path[MAXPGPATH], hex[65];
	uint8 hash[32];
	size_t len;

	if (cluster_control_root_v2_decode(bytes, 66048, v2_storage, TEST_SYSID, &decoded) != 0)
		abort();
	memset(&id, 0, sizeof(id));
	id.system_identifier = TEST_SYSID + (foreign ? 1 : 0);
	id.database_incarnation = decoded.header.v2.database_incarnation;
	id.generation = decoded.header.v2.config_generation;
	id.configured[0] = decoded.header.v2.configured[0];
	id.configured[1] = decoded.header.v2.configured[1];
	memcpy(id.storage_uuid, decoded.header.storage_uuid, 16);
	memcpy(id.authority_uuid, decoded.header.authority_uuid, 16);
	if (cluster_shared_config_encode(&id, &entry, 1, config, sizeof(config), &len, hash) != 0)
		abort();
	path_for(path, sizeof(path), "global/config_images");
	if (mkdir(path, 0700) != 0 && errno != EEXIST)
		abort();
	for (int i = 0; i < 32; ++i)
		snprintf(hex + 2 * i, 3, "%02x", hash[i]);
	snprintf(path, sizeof(path), "%s/global/config_images/47-%s.conf", test_root, hex);
	write_all_or_abort(path, (const uint8 *)config, len);
	memcpy(bytes + 256, hash, 32);
	v2_checksums(bytes);
}

static void
v2_view_fixture(uint8 bytes[66048], ControlFileData *native, ClusterCfImageStage *stage)
{
	static pg_time_t native_time = 900;
	char path[MAXPGPATH];

	wipe_root_files();
	memset(native, 0, sizeof(*native));
	native->system_identifier = TEST_SYSID;
	native->pg_control_version = PG_CONTROL_VERSION;
	native->catalog_version_no = CATALOG_VERSION_NO;
	native->time = native_time++;
	native->state = DB_SHUTDOWNED;
	native->checkPoint = UINT64_C(0x9000000);
	v2_install_native(native, stage);
	v2_fixture(bytes);
	memcpy(bytes + 296, stage->image_sha256, 32);
	v2_checksums(bytes);
	v2_install_config(bytes, false);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
}

static bool
v2_outputs_zero(const ControlRootImage *root, const ControlFileData *common,
				const ClusterControlRootFileToken *token)
{
	return v2_zero(root, sizeof(*root)) && v2_zero(common, sizeof(*common))
		   && v2_zero(token, sizeof(*token));
}

UT_TEST(test_v2_view_selects_exact_hash_not_decoy_or_projection)
{
	uint8 bytes[66048], hash[32];
	ControlRootImage root;
	ControlFileData native, out, decoy;
	ClusterCfImageStage stage, other;
	ClusterControlRootFileToken token;

	v2_view_fixture(bytes, &native, &stage);
	decoy = native;
	decoy.time += 99;
	v2_install_native(&decoy, &other);
	cluster_cf_authority_write(&decoy);
	test_cf_mode = ShareLock;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(token.format_version, 2);
	UT_ASSERT_EQ(token.file_txn_seq, 7);
	UT_ASSERT_EQ(out.time, native.time);
	UT_ASSERT_EQ(out.checkPoint, native.checkPoint);
	UT_ASSERT(memcmp(root.bytes, bytes, sizeof(bytes)) == 0);
	sha256_bytes(bytes, sizeof(bytes), hash);
	UT_ASSERT(memcmp(token.image_sha256, hash, sizeof(hash)) == 0);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	/* Common native state is NOT the database/thread admission state. */
	UT_ASSERT_EQ(out.state, DB_SHUTDOWNED);
	UT_ASSERT_EQ(root.header.v2.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
}

UT_TEST(test_v2_view_requires_clusterwide_lock_and_verified_storage)
{
	uint8 bytes[66048], foreign[16];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	int fault;

	v2_view_fixture(bytes, &native, &stage);
	for (fault = 0; fault < 3; ++fault) {
		test_cf_grant = fault != 0;
		test_cf_clusterwide = fault != 1;
		test_contract
			= fault == 2 ? CLUSTER_CF_CONTRACT_UNVERIFIED : CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
		memset(&root, 0xee, sizeof(root));
		memset(&out, 0xee, sizeof(out));
		memset(&token, 0xee, sizeof(token));
		UT_ASSERT(
			cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token)
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	memcpy(foreign, v2_storage, sizeof(foreign));
	foreign[0]++;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(foreign, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT_EQ(cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID + 1, &root,
															 &out, &token),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_valid_backup_never_substitutes_for_current)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	char primary[MAXPGPATH];

	v2_view_fixture(bytes, &native, &stage);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	bytes[200] ^= 1;
	write_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT(unlink(primary) == 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_BAK_BLOCKED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_backup_divergence_and_degraded_are_distinct)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	char bak[MAXPGPATH];

	v2_view_fixture(bytes, &native, &stage);
	path_for(bak, sizeof(bak), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	put_u64_le(bytes + 16, 8);
	v2_checksums(bytes);
	write_all_or_abort(bak, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_COPY_DIVERGENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	put_u64_le(bytes + 16, 7);
	put_u64_le(bytes + 336, 73);
	v2_checksums(bytes);
	write_all_or_abort(bak, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_COPY_DIVERGENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT(unlink(bak) == 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY_DEGRADED);
	UT_ASSERT_EQ(out.time, native.time);
	UT_ASSERT_EQ(token.format_version, 2);
}

UT_TEST(test_v2_view_selected_object_failure_clears_valid_root)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;
	char hash[65], object[MAXPGPATH];
	int i;

	v2_view_fixture(bytes, &native, &stage);
	for (i = 0; i < 32; ++i)
		snprintf(hash + 2 * i, 3, "%02x", stage.image_sha256[i]);
	snprintf(object, sizeof(object), "%s/global/control_images/53-%s.bin", test_root, hash);
	memset(bytes, 0x5a, PG_CONTROL_FILE_SIZE);
	write_all_or_abort(object, bytes, PG_CONTROL_FILE_SIZE);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	UT_ASSERT(unlink(object) == 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_token_covers_whole_root_not_only_control_hash)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken before, after;
	char primary[MAXPGPATH];

	v2_view_fixture(bytes, &native, &stage);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &before),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	put_u64_le(bytes + 16, 8);
	put_u64_le(bytes + 336, 65);
	v2_checksums(bytes);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &after),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(after.file_txn_seq, before.file_txn_seq + 1);
	UT_ASSERT(memcmp(before.image_sha256, after.image_sha256, 32) != 0);
	UT_ASSERT_EQ(out.time, native.time);
}

UT_TEST(test_v2_view_rejects_v1_and_invalid_input_without_conversion)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootMigrationImage legacy;
	ClusterControlRootMigrationRoundV1 round;
	ClusterControlRootFileToken token;
	char primary[MAXPGPATH];

	wipe_root_files();
	UT_ASSERT_EQ(create_prepared(&legacy, &round, &token), CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(primary, bytes, sizeof(bytes));
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_BAD_VERSION);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	read_all_or_abort(primary, root.bytes, sizeof(root.bytes));
	UT_ASSERT(memcmp(root.bytes, bytes, sizeof(bytes)) == 0);
	UT_ASSERT_EQ(cluster_control_root_v2_read_control_locked(NULL, TEST_SYSID, &root, &out, &token),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_view_single_node_does_not_bypass_shared_storage_qualification)
{
	uint8 bytes[66048];
	ControlRootImage root;
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ClusterControlRootFileToken token;

	v2_view_fixture(bytes, &native, &stage);
	test_node_count = 1;
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_STORAGE_CONTRACT_UNVERIFIED);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	test_contract = CLUSTER_CF_CONTRACT_CROSSNODE_VERIFIED;
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(out.time, native.time);
}

/* PGRAC: composition consumes independently encoded roots and actual anchor
 * objects. The anchor's standalone tests pin its independent byte fixture.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_anchor_object(uint8 bytes[66048], const ClusterRecoveryAnchorV2 *anchor,
				 const ClusterControlRootIdentity *path_identity, char path[MAXPGPATH])
{
	uint8 image[512], hash[32];
	char dir[MAXPGPATH], hex[65];
	size_t i;
	int node = path_identity->origin_node_id;

	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(anchor, image), 0);
	sha256_bytes(image, sizeof(image), hash);
	for (i = 0; i < 32; ++i)
		snprintf(hex + i * 2, 3, "%02x", hash[i]);
	path_for(dir, sizeof(dir), "global/anchor_images");
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_%u", cluster_shared_data_dir,
			 path_identity->origin_thread_id);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT,
			 cluster_shared_data_dir, path_identity->origin_thread_id,
			 path_identity->origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(path, MAXPGPATH, "%s/anchor_" UINT64_FORMAT "-%s.bin", dir, anchor->anchor_generation,
			 hex);
	write_all_or_abort(path, image, sizeof(image));
	memcpy(bytes + 512 + node * 512 + 264, hash, 32);
	v2_checksums(bytes);
}

static void
v2_write_roots(uint8 bytes[66048])
{
	char path[MAXPGPATH];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
	write_all_or_abort(path, bytes, 66048);
}

static void
v2_claim_path(const ClusterControlRootIdentity *id, char path[MAXPGPATH])
{
	snprintf(path, MAXPGPATH, "%s/thread_%u/generation_" UINT64_FORMAT "/pgrac_thread.claim",
			 cluster_wal_threads_dir, id->origin_thread_id, id->origin_owner_incarnation);
}

/* PGRAC: independently create the selected claim before constructing anchors.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_claim_object(uint8 bytes[66048], int node, ControlRootImage *root)
{
	uint8 image[112] = { 0 }, hash[32];
	ClusterControlRootIdentity *id = &root->records[node].identity;
	char dir[MAXPGPATH], path[MAXPGPATH];

	put_u32_le(image, UINT32_C(0x50475443));
	put_u16_le(image + 4, 2);
	put_u16_le(image + 6, id->origin_thread_id);
	put_u32_le(image + 8, id->origin_node_id);
	put_u64_le(image + 16, id->system_identifier);
	put_u64_le(image + 24, root->header.v2.database_incarnation);
	put_u64_le(image + 32, id->origin_owner_incarnation);
	memcpy(image + 40, id->storage_uuid, 16);
	memcpy(image + 56, id->authority_uuid, 16);
	put_u64_le(image + 72, id->root_lineage_seq);
	put_u64_le(image + 80, id->thread_claim_created_at);
	put_u64_le(image + 88, root->header.v2.config_generation - 1);
	put_u64_le(image + 96, 1);
	id->thread_claim_crc32c = image_crc(image, 104);
	put_u32_le(image + 104, id->thread_claim_crc32c);
	sha256_bytes(image, sizeof(image), hash);
	memcpy(root->refs[node].claim_sha256, hash, 32);
	memcpy(bytes + 512 + node * 512 + 296, hash, 32);
	put_u32_le(bytes + 512 + node * 512 + 168, id->thread_claim_crc32c);
	v2_checksums(bytes);
	snprintf(dir, sizeof(dir), "%s/thread_%u", cluster_wal_threads_dir, id->origin_thread_id);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/thread_%u/generation_" UINT64_FORMAT, cluster_wal_threads_dir,
			 id->origin_thread_id, id->origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	v2_claim_path(id, path);
	write_all_or_abort(path, image, sizeof(image));
}

static void
v2_thread_fixture(uint8 bytes[66048], ClusterRecoveryAnchorV2 anchors[2])
{
	ControlFileData native;
	ClusterCfImageStage stage;
	ControlRootImage root;
	char path[MAXPGPATH];
	int i;

	v2_view_fixture(bytes, &native, &stage);
	native.MaxConnections = 300;
	native.checkPointCopy.nextOid = 60001;
	v2_install_native(&native, &stage);
	memcpy(bytes + 296, stage.image_sha256, 32);
	v2_checksums(bytes);
	UT_ASSERT_EQ(cluster_control_root_v2_decode(bytes, 66048, v2_storage, TEST_SYSID, &root), 0);
	memset(anchors, 0, 2 * sizeof(*anchors));
	for (i = 0; i < 2; ++i) {
		int node = i == 0 ? 0 : 127;
		ClusterRecoveryAnchorV2 *a = &anchors[i];

		v2_claim_object(bytes, node, &root);
		a->identity = root.records[node].identity;
		a->database_incarnation = root.header.v2.database_incarnation;
		a->config_generation = root.header.v2.config_generation;
		a->anchor_generation = root.refs[node].anchor_generation;
		memcpy(a->claim_sha256, root.refs[node].claim_sha256, 32);
		a->state = DB_IN_PRODUCTION;
		a->checkpoint_copy.redo = root.records[node].checkpoint_lower_lsn;
		a->checkpoint = a->checkpoint_copy.redo + 128;
		a->write_time = 1001 + node;
		a->checkpoint_copy.ThisTimeLineID = root.records[node].checkpoint_tli;
		a->checkpoint_copy.PrevTimeLineID = root.records[node].checkpoint_tli;
		a->checkpoint_copy.nextOid = 40 + node; /* deliberately not common */
		a->min_recovery_point = a->checkpoint_copy.redo + 64;
		a->min_recovery_tli = root.records[node].checkpoint_tli;
		a->unlogged_lsn = UINT64_C(0x2000000) + node;
		a->max_connections = 300 + node;
		a->wal_level = 1;
		a->max_worker_processes = 16;
		a->max_wal_senders = 5;
		a->max_locks_per_xact = 64;
		v2_anchor_object(bytes, a, &a->identity, path);
	}
	v2_write_roots(bytes);
}

UT_TEST(test_v2_thread_view_selects_each_exact_thread)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	pg_crc32c crc;
	int i;

	v2_thread_fixture(bytes, anchors);
	test_cf_mode = ShareLock;
	for (i = 0; i < 2; ++i) {
		UT_ASSERT_EQ(
			cluster_control_root_v2_read_thread_locked(&anchors[i].identity, &root, &out, &token),
			0);
		UT_ASSERT_EQ(out.checkPoint, anchors[i].checkpoint);
		UT_ASSERT_EQ(out.minRecoveryPoint, anchors[i].min_recovery_point);
		UT_ASSERT_EQ(out.unloggedLSN, anchors[i].unlogged_lsn);
		UT_ASSERT_EQ(out.state, DB_IN_PRODUCTION);
		UT_ASSERT_EQ(out.MaxConnections, i == 0 ? 300 : 427);
		UT_ASSERT_EQ(out.checkPointCopy.nextOid, 60001);
		UT_ASSERT_EQ(token.file_txn_seq, 7);
		UT_ASSERT_EQ(token.format_version, 2);
		INIT_CRC32C(crc);
		COMP_CRC32C(crc, &out, offsetof(ControlFileData, crc));
		FIN_CRC32C(crc);
		UT_ASSERT(EQ_CRC32C(crc, out.crc));
	}
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
}

UT_TEST(test_v2_thread_view_rejects_stale_caller_and_absent_record)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterControlRootIdentity self;
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;

	v2_thread_fixture(bytes, anchors);
	self = anchors[0].identity;
	self.origin_owner_incarnation++;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &out, &token),
				 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	self = anchors[0].identity;
	self.origin_thread_id = 2;
	self.origin_node_id = 1;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &out, &token),
				 CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_thread_view_clears_root_after_missing_anchor)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];

	v2_thread_fixture(bytes, anchors);
	v2_anchor_object(bytes, &anchors[0], &anchors[0].identity, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(
		cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token),
		CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT(v2_outputs_zero(&root, &out, &token));
}

UT_TEST(test_v2_thread_view_rejects_selected_foreign_or_inconsistent_anchor)
{
	uint8 bytes[66048];
	ClusterRecoveryAnchorV2 anchors[2];
	ClusterControlRootIdentity self;
	ControlRootImage root;
	ControlFileData out;
	ClusterControlRootFileToken token;
	char path[MAXPGPATH];
	int fault;

	for (fault = 0; fault < 3; ++fault) {
		v2_thread_fixture(bytes, anchors);
		self = anchors[0].identity;
		if (fault == 0)
			anchors[0].database_incarnation++;
		else if (fault == 1)
			anchors[0].checkpoint_copy.redo += 8192;
		else
			anchors[0].checkpoint_copy.ThisTimeLineID++;
		v2_anchor_object(bytes, &anchors[0], &self, path);
		v2_write_roots(bytes);
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &out, &token),
					 CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

UT_TEST(test_v2_thread_view_requires_physical_selected_claim)
{
	for (int fault = 0; fault < 5; ++fault) {
		uint8 bytes[66048], claim[113];
		ClusterRecoveryAnchorV2 anchors[2];
		ControlRootImage root;
		ControlFileData out;
		ClusterControlRootFileToken token;
		char path[MAXPGPATH];
		ClusterControlRootResult expected;

		v2_thread_fixture(bytes, anchors);
		v2_claim_path(&anchors[0].identity, path);
		read_all_or_abort(path, claim, 112);
		if (fault == 0) {
			UT_ASSERT_EQ(unlink(path), 0);
			expected = CLUSTER_CONTROL_ROOT_ABSENT;
		} else if (fault == 1) {
			claim[32] ^= 1;
			write_all_or_abort(path, claim, 112);
			expected = CLUSTER_CONTROL_ROOT_BAD_RECORD_CRC;
		} else if (fault == 2) {
			claim[4] = 1;
			put_u32_le(claim + 104, image_crc(claim, 104));
			write_all_or_abort(path, claim, 112);
			expected = CLUSTER_CONTROL_ROOT_BAD_VERSION;
		} else if (fault == 3) {
			claim[112] = 0;
			write_all_or_abort(path, claim, 113);
			expected = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		} else {
			++claim[32];
			put_u32_le(claim + 104, image_crc(claim, 104));
			write_all_or_abort(path, claim, 112);
			expected = CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
		}
		memset(&root, 0xa5, sizeof(root));
		memset(&out, 0xa5, sizeof(out));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT_EQ(
			cluster_control_root_v2_read_thread_locked(&anchors[0].identity, &root, &out, &token),
			expected);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

/* PGRAC: exact-filesystem checkpoint publication, no fake positive publisher.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static XLogRecPtr test_checkpoint_end;
static uint32 test_checkpoint_crc;
static ClusterWalDurablePrefixRef test_checkpoint_prefix_ref;
static ClusterWalDurablePrefix test_checkpoint_prefix;
static char test_checkpoint_prefix_path[MAXPGPATH];

static void
v2_checkpoint_prefix_write(void)
{
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&test_checkpoint_prefix_ref,
												   &test_checkpoint_prefix, bytes),
				 0);
	write_all_or_abort(test_checkpoint_prefix_path, bytes, sizeof(bytes));
}

static void
v2_checkpoint_wal_path(const ClusterControlRootIdentity *self, XLogRecPtr position, TimeLineID tli,
					   char path[MAXPGPATH])
{
	char filename[MAXFNAMELEN];
	XLogSegNo segno;
	XLByteToSeg(position, segno, wal_segment_size);
	XLogFileName(filename, tli, segno, wal_segment_size);
	snprintf(path, MAXPGPATH, "%s/thread_%u/generation_" UINT64_FORMAT "/%s",
			 cluster_wal_threads_dir, self->origin_thread_id, self->origin_owner_incarnation,
			 filename);
}

/* Actual native record framing, including page and segment continuation. No
 * replacement of XLogReader or its record/CRC validation. Faults alter bytes. */
static void
v2_checkpoint_wal_record(const ClusterControlRootIdentity *self, const ControlFileData *candidate,
						 int fault)
{
	uint8 record_bytes[SizeOfXLogRecord + 2 + sizeof(CheckPoint)];
	XLogRecord record;
	CheckPoint checkpoint = candidate->checkPointCopy;
	XLogRecPtr position = candidate->checkPoint;
	XLogSegNo previous_segment = UINT64_MAX;
	size_t used = 0;
	pg_crc32c crc;

	memset(&record, 0, sizeof(record));
	memset(record_bytes, 0, sizeof(record_bytes));
	if (fault == 6)
		checkpoint.time++;
	record.xl_tot_len = sizeof(record_bytes);
	record.xl_prev = candidate->checkPointCopy.redo;
	record.xl_info = fault == 5 ? XLOG_CHECKPOINT_SHUTDOWN : XLOG_CHECKPOINT_ONLINE;
	record.xl_rmid = RM_XLOG_ID;
	record_bytes[SizeOfXLogRecord] = XLR_BLOCK_ID_DATA_SHORT;
	record_bytes[SizeOfXLogRecord + 1] = sizeof(CheckPoint);
	memcpy(record_bytes + SizeOfXLogRecord + 2, &checkpoint, sizeof(checkpoint));
	INIT_CRC32C(crc);
	COMP_CRC32C(crc, record_bytes + SizeOfXLogRecord, sizeof(record_bytes) - SizeOfXLogRecord);
	COMP_CRC32C(crc, &record, offsetof(XLogRecord, xl_crc));
	FIN_CRC32C(crc);
	record.xl_crc = crc;
	test_checkpoint_crc = crc;
	memcpy(record_bytes, &record, SizeOfXLogRecord);
	if (fault == 4)
		record_bytes[sizeof(record_bytes) - 1] ^= 1;
	while (used < sizeof(record_bytes)) {
		uint8 page[XLOG_BLCKSZ];
		XLogLongPageHeaderData header;
		XLogRecPtr page_start = position - position % XLOG_BLCKSZ;
		XLogSegNo segno;
		size_t offset = position % XLOG_BLCKSZ, count;
		char path[MAXPGPATH];
		int fd;
		XLByteToSeg(position, segno, wal_segment_size);
		v2_checkpoint_wal_path(self, position, candidate->checkPointCopy.ThisTimeLineID, path);
		fd = open(path, O_RDWR | O_CREAT | (segno != previous_segment ? O_TRUNC : 0), 0600);
		if (fd < 0 || ftruncate(fd, wal_segment_size) != 0)
			abort();
		memset(&header, 0, sizeof(header));
		header.std.xlp_magic = XLOG_PAGE_MAGIC;
		header.std.xlp_info = XLP_LONG_HEADER;
		header.std.xlp_tli = candidate->checkPointCopy.ThisTimeLineID + (fault == 2);
		header.std.xlp_thread_id = fault == 1 ? self->origin_thread_id + 1 : self->origin_thread_id;
		header.std.xlp_pageaddr = segno * wal_segment_size;
		header.xlp_sysid = self->system_identifier + (fault == 3);
		header.xlp_seg_size = wal_segment_size;
		header.xlp_xlog_blcksz = XLOG_BLCKSZ;
		if (segno != previous_segment) {
			memset(page, 0, sizeof(page));
			memcpy(page, &header, SizeOfXLogLongPHD);
			if (pwrite(fd, page, sizeof(page), 0) != sizeof(page))
				abort();
		}
		memset(page, 0, sizeof(page));
		header.std.xlp_pageaddr = page_start;
		if (page_start % wal_segment_size != 0)
			header.std.xlp_info = 0;
		if (used != 0) {
			header.std.xlp_info |= XLP_FIRST_IS_CONTRECORD;
			header.std.xlp_rem_len = sizeof(record_bytes) - used;
			offset = page_start % wal_segment_size == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD;
		}
		memcpy(page, &header,
			   page_start % wal_segment_size == 0 ? SizeOfXLogLongPHD : SizeOfXLogShortPHD);
		count = Min(sizeof(record_bytes) - used, XLOG_BLCKSZ - offset);
		memcpy(page + offset, record_bytes + used, count);
		if (pwrite(fd, page, sizeof(page), page_start % wal_segment_size) != sizeof(page)
			|| close(fd) != 0)
			abort();
		used += count;
		position = page_start + offset + count;
		previous_segment = segno;
	}
	test_checkpoint_end = MAXALIGN(position);
	test_flush = test_checkpoint_end;
	test_checkpoint_prefix
		= (ClusterWalDurablePrefix){ 11, test_checkpoint_end, candidate->checkPoint,
									 test_checkpoint_crc };
	v2_checkpoint_prefix_write();
}

static void
v2_checkpoint_fixture(uint8 before[66048], ClusterControlRootIdentity *self,
					  ControlFileData *candidate)
{
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	ClusterControlRootFileToken token;
	char dir[MAXPGPATH];

	v2_thread_fixture(before, anchors);
	*self = anchors[0].identity;
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(self, &root, candidate, &token), 0);
	memset(&test_checkpoint_prefix_ref, 0, sizeof(test_checkpoint_prefix_ref));
	test_checkpoint_prefix_ref.claim.identity = *self;
	test_checkpoint_prefix_ref.claim.database_incarnation = root.header.v2.database_incarnation;
	test_checkpoint_prefix_ref.claim.max_config_generation = root.header.v2.config_generation;
	memcpy(test_checkpoint_prefix_ref.claim.claim_sha256, root.refs[0].claim_sha256, 32);
	test_checkpoint_prefix_ref.timeline = candidate->checkPointCopy.ThisTimeLineID;
	snprintf(dir, sizeof(dir), "%s/thread_%u/generation_" UINT64_FORMAT "/durable_prefix",
			 cluster_wal_threads_dir, self->origin_thread_id, self->origin_owner_incarnation);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(test_checkpoint_prefix_path, sizeof(test_checkpoint_prefix_path), "%s/current", dir);
	candidate->checkPoint += 8192;
	candidate->checkPointCopy.redo += 8192;
	candidate->time++;
	candidate->checkPointCopy.time++;
	INIT_CRC32C(candidate->crc);
	COMP_CRC32C(candidate->crc, candidate, offsetof(ControlFileData, crc));
	FIN_CRC32C(candidate->crc);
	snprintf(dir, sizeof(dir), "%s/global/anchor_images/thread_1/generation_99/.staging",
			 test_root);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	test_checkpoint_mode = true;
	MyAuxProcType = CheckpointerProcess;
	cluster_node_id = 0;
	cluster_enabled = cluster_controlfile_shared_authority = true;
	test_self_incarnation = test_membership_incarnation = self->origin_owner_incarnation;
	test_own_thread = self->origin_thread_id;
	test_member_state = CLUSTER_MEMBER_MEMBER;
	test_epoch = 97;
	test_serving = test_fence = test_wal_validated = true;
	test_prebump = false;
	v2_checkpoint_wal_record(self, candidate, 0);
	test_flush_tli = candidate->checkPointCopy.ThisTimeLineID;
	test_cf_mode = NoLock;
	test_cf_lock_calls = test_durable_rename_calls = 0;
	test_checkpoint_outer_cf = false;
}

static ClusterControlRootResult
v2_checkpoint_publish(const ClusterControlRootIdentity *self, const ControlFileData *candidate,
					  ClusterControlRootSnapshot *out, ClusterControlRootFileToken *token)
{
	return cluster_control_root_v2_checkpoint_publish(self, candidate, test_checkpoint_end,
													  test_checkpoint_crc, out, token);
}

static void
v2_assert_primary_unchanged(const uint8 before[66048])
{
	uint8 bytes[66048];
	char path[MAXPGPATH];

	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, bytes, sizeof(bytes));
	UT_ASSERT(memcmp(before, bytes, sizeof(bytes)) == 0);
}

static void
v2_assert_anchor_staging_empty(void)
{
	char path[MAXPGPATH];
	DIR *dir;
	struct dirent *entry;
	int count = 0;

	snprintf(path, sizeof(path), "%s/global/anchor_images/thread_1/generation_99/.staging",
			 test_root);
	dir = opendir(path);
	UT_ASSERT(dir != NULL);
	if (dir == NULL)
		return;
	while ((entry = readdir(dir)) != NULL)
		if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
			count++;
	UT_ASSERT_EQ(closedir(dir), 0);
	UT_ASSERT_EQ(count, 0);
}

UT_TEST(test_v2_checkpoint_advances_one_thread_and_preserves_common)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlRootImage root;
	ControlFileData candidate, view;

	v2_checkpoint_fixture(before, &self, &candidate);
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
	UT_ASSERT_EQ(out.checkpoint_lower_lsn, candidate.checkPointCopy.redo);
	UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
	UT_ASSERT_EQ(token.file_txn_seq, 8);
	UT_ASSERT_EQ(token.format_version, 2);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	UT_ASSERT(test_walr_begin_order < test_cf_acquire_order);
	UT_ASSERT(test_last_rename_order < test_cf_release_order);
	UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
	UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
	UT_ASSERT_EQ(root.refs[0].anchor_generation, 67);
	UT_ASSERT(memcmp(root.bytes + 196, before + 196, 180) == 0);
	UT_ASSERT(memcmp(root.bytes + 1024, before + 1024, 66048 - 1024) == 0);
	UT_ASSERT_EQ(view.checkPointCopy.nextOid, 60001);
	UT_ASSERT_EQ(root.records[0].root_publish_seq, 11);
	UT_ASSERT_EQ(root.records[0].identity.origin_owner_incarnation, 99);
	v2_assert_anchor_staging_empty();
}

UT_TEST(test_v2_checkpoint_preserves_historical_parameter_requirements)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlRootImage root;
	ControlFileData candidate, view;

	v2_checkpoint_fixture(before, &self, &candidate);
	for (int lower = 0; lower < 2; ++lower) {
		candidate.MaxConnections = lower ? 100 : 811;
		candidate.max_worker_processes = lower ? 1 : 28;
		candidate.max_wal_senders = lower ? 0 : 19;
		candidate.max_prepared_xacts = lower ? 0 : 13;
		candidate.max_locks_per_xact = lower ? 1 : 259;
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
		UT_ASSERT_EQ(view.MaxConnections, 811);
		UT_ASSERT_EQ(view.max_worker_processes, 28);
		UT_ASSERT_EQ(view.max_wal_senders, 19);
		UT_ASSERT_EQ(view.max_prepared_xacts, 13);
		UT_ASSERT_EQ(view.max_locks_per_xact, 259);
		UT_ASSERT_EQ(view.wal_level, 1);
		UT_ASSERT(!view.wal_log_hints && !view.track_commit_timestamp);
		UT_ASSERT(memcmp(root.bytes + 196, before + 196, 180) == 0);
		UT_ASSERT(memcmp(root.bytes + 1024, before + 1024, 66048 - 1024) == 0);
		v2_assert_anchor_staging_empty();
		candidate = view;
		candidate.checkPoint += 8192;
		candidate.checkPointCopy.redo += 8192;
		v2_checkpoint_wal_record(&self, &candidate, 0);
	}
}

UT_TEST(test_v2_checkpoint_requires_actual_wal_record)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		char path[MAXPGPATH];
		v2_checkpoint_fixture(before, &self, &candidate);
		v2_checkpoint_wal_path(&self, candidate.checkPoint, candidate.checkPointCopy.ThisTimeLineID,
							   path);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(path), 0);
		else if (fault <= 6)
			v2_checkpoint_wal_record(&self, &candidate, fault);
		else if (fault == 7)
			UT_ASSERT_EQ(truncate(path, 1), 0);
		else if (fault == 8)
			test_checkpoint_crc ^= 1;
		else {
			test_checkpoint_end += 8;
			test_flush = test_checkpoint_end;
		}
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
	}
}

UT_TEST(test_v2_checkpoint_requires_exact_durable_prefix)
{
	for (int fault = 0; fault < 10; ++fault) {
		uint8 before[66048], bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		char moved[MAXPGPATH];
		v2_checkpoint_fixture(before, &self, &candidate);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		else if (fault == 1) {
			read_all_or_abort(test_checkpoint_prefix_path, bytes, sizeof(bytes));
			bytes[252] ^= 1;
			write_all_or_abort(test_checkpoint_prefix_path, bytes, sizeof(bytes));
		} else if (fault == 2) {
			test_checkpoint_prefix_ref.timeline++;
			v2_checkpoint_prefix_write();
		} else if (fault == 3) {
			test_checkpoint_prefix = (ClusterWalDurablePrefix){ 1, 0, 0, 0 };
			v2_checkpoint_prefix_write();
		} else if (fault == 4) {
			test_checkpoint_prefix.exclusive_end -= 8;
			v2_checkpoint_prefix_write();
		} else if (fault == 5)
			UT_ASSERT_EQ(truncate(test_checkpoint_prefix_path, 12), 0);
		else if (fault == 6) {
			snprintf(moved, sizeof(moved), "%s.moved", test_checkpoint_prefix_path);
			UT_ASSERT_EQ(rename(test_checkpoint_prefix_path, moved), 0);
			UT_ASSERT_EQ(symlink(moved, test_checkpoint_prefix_path), 0);
		} else {
			if (fault == 7)
				test_checkpoint_prefix.record_crc ^= 1;
			else if (fault == 8)
				test_checkpoint_prefix.record_start += 8;
			else
				test_checkpoint_prefix.exclusive_end += 128;
			v2_checkpoint_prefix_write();
		}
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
		if (fault == 6) {
			UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
			UT_ASSERT_EQ(rename(moved, test_checkpoint_prefix_path), 0);
		}
	}
}

static int test_prefix_race;

static void
v2_checkpoint_prefix_race(void)
{
	test_checkpoint_x_hook = NULL;
	if (test_prefix_race == 0)
		test_checkpoint_prefix.sequence--;
	else if (test_prefix_race == 1)
		test_checkpoint_prefix.record_crc ^= 1;
	else if (test_prefix_race == 2)
		test_checkpoint_prefix.sequence++;
	else if (test_prefix_race == 3) {
		test_checkpoint_prefix.sequence++;
		test_checkpoint_prefix.exclusive_end += 128;
	} else if (test_prefix_race == 4) {
		UT_ASSERT_EQ(unlink(test_checkpoint_prefix_path), 0);
		return;
	} else {
		/* A checkpoint need not consume these later records: it must only
		 * retain coverage of its own independently verified native record. */
		test_checkpoint_prefix.sequence += 3;
		test_checkpoint_prefix.record_start = test_checkpoint_prefix.exclusive_end + 16;
		test_checkpoint_prefix.exclusive_end += 128;
	}
	v2_checkpoint_prefix_write();
}

UT_TEST(test_v2_checkpoint_rechecks_durable_prefix)
{
	for (int fault = 0; fault < 7; ++fault) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		v2_checkpoint_fixture(before, &self, &candidate);
		test_prefix_race = fault;
		test_checkpoint_x_hook = v2_checkpoint_prefix_race;
		if (fault == 6) {
			v2_checkpoint_prefix_race();
			/* An already-ahead, nonoverlapping promise is legitimate too. */
		}
		if (fault >= 5) {
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
			UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
		} else {
			UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
			UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
			v2_assert_primary_unchanged(before);
		}
		v2_assert_anchor_staging_empty();
		UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
		UT_ASSERT_EQ(test_cf_mode, NoLock);
	}
}

UT_TEST(test_v2_checkpoint_wal_continuation)
{
	for (int segment = 0; segment < 2; ++segment) {
		uint8 before[66048];
		ClusterControlRootIdentity self;
		ClusterControlRootSnapshot out;
		ClusterControlRootFileToken token;
		ControlFileData candidate;
		v2_checkpoint_fixture(before, &self, &candidate);
		candidate.checkPoint
			= segment ? (candidate.checkPoint / wal_segment_size + 2) * wal_segment_size - 48
					  : (candidate.checkPoint / XLOG_BLCKSZ + 2) * XLOG_BLCKSZ - 48;
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		v2_checkpoint_wal_record(&self, &candidate, 0);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token), 0);
		UT_ASSERT_EQ(out.tail_last_record_lsn, candidate.checkPoint);
		UT_ASSERT_EQ(out.validated_tail_lsn_exclusive, test_checkpoint_end);
		UT_ASSERT_EQ(out.tail_last_record_crc32c, test_checkpoint_crc);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_rejects_invalid_parameter_before_max)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	for (int fault = 0; fault < 8; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			candidate.MaxConnections = 0;
			break;
		case 1:
			candidate.MaxConnections = -1;
			break;
		case 2:
			candidate.max_worker_processes = -1;
			break;
		case 3:
			candidate.max_wal_senders = -1;
			break;
		case 4:
			candidate.max_prepared_xacts = -1;
			break;
		case 5:
			candidate.max_locks_per_xact = 0;
			break;
		case 6:
			candidate.wal_level = -1;
			break;
		case 7:
			candidate.wal_level = 3;
			break;
		}
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
					 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_cannot_invent_parameter_transition_proof)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	for (int fault = 0; fault < 4; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			candidate.wal_level = 0;
			break;
		case 1:
			candidate.wal_level = 2;
			break;
		case 2:
			candidate.wal_log_hints = true;
			break;
		case 3:
			candidate.track_commit_timestamp = true;
			break;
		}
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
					 CLUSTER_CONTROL_ROOT_RANGE_INVALID);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		UT_ASSERT(v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_rejects_non_owner_facts_before_io)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	int fault;

	for (fault = 0; fault < 12; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			MyAuxProcType = NotAnAuxProcess;
			break;
		case 1:
			test_self_incarnation++;
			break;
		case 2:
			test_membership_incarnation++;
			break;
		case 3:
			test_member_state = CLUSTER_MEMBER_ABSENT;
			break;
		case 4:
			test_fence = false;
			break;
		case 5:
			test_serving = false;
			break;
		case 6:
			test_prebump = true;
			break;
		case 7:
			test_wal_validated = false;
			break;
		case 8:
			cluster_enabled = false;
			break;
		case 9:
			cluster_controlfile_shared_authority = false;
			break;
		case 10:
			self.origin_node_id = 127;
			break;
		case 11:
			test_checkpoint_outer_cf = true;
			break;
		}
		memset(&out, 0xab, sizeof(out));
		memset(&token, 0xcd, sizeof(token));
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
	}
}

UT_TEST(test_v2_checkpoint_rejects_unflushed_wrong_tli_and_bad_inputs)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	int fault;

	for (fault = 0; fault < 9; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			test_flush--;
			break;
		case 1:
			test_flush_tli++;
			break;
		case 2:
			candidate.state = DB_SHUTDOWNED;
			break;
		case 3:
			candidate.backupEndRequired = true;
			break;
		case 4:
			candidate.backupStartPoint = 1;
			break;
		case 5:
			candidate.minRecoveryPoint = 0;
			break;
		case 6:
			candidate.checkPointCopy.redo -= 8192;
			break;
		case 7:
			candidate.system_identifier++;
			break;
		case 8:
			candidate.checkPointCopy.ThisTimeLineID++;
			test_flush_tli++;
			break;
		}
		/* Do not let a stale CRC mask the intended semantic rejection. */
		INIT_CRC32C(candidate.crc);
		COMP_CRC32C(candidate.crc, &candidate, offsetof(ControlFileData, crc));
		FIN_CRC32C(candidate.crc);
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

static void
v2_checkpoint_replace_wal_generation(void)
{
	char generation[MAXPGPATH], moved[MAXPGPATH], claim[MAXPGPATH];
	uint8 bytes[112];
	test_checkpoint_x_hook = NULL;
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_99", cluster_wal_threads_dir);
	snprintf(moved, sizeof(moved), "%s/thread_1/generation_99.moved", cluster_wal_threads_dir);
	snprintf(claim, sizeof(claim), "%s/%s", generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	read_all_or_abort(claim, bytes, sizeof(bytes));
	if (rename(generation, moved) != 0 || mkdir(generation, 0700) != 0)
		abort();
	/* Same exact claim passes the final root read, but its WAL is not the
	 * pinned directory whose bytes were verified. */
	write_all_or_abort(claim, bytes, sizeof(bytes));
}

UT_TEST(test_v2_checkpoint_rejects_replaced_wal_directory)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	char generation[MAXPGPATH], moved[MAXPGPATH], claim[MAXPGPATH];
	v2_checkpoint_fixture(before, &self, &candidate);
	test_checkpoint_x_hook = v2_checkpoint_replace_wal_generation;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT_EQ(test_cf_mode, NoLock);
	snprintf(generation, sizeof(generation), "%s/thread_1/generation_99", cluster_wal_threads_dir);
	snprintf(moved, sizeof(moved), "%s/thread_1/generation_99.moved", cluster_wal_threads_dir);
	snprintf(claim, sizeof(claim), "%s/%s", generation, CLUSTER_WAL_THREAD_CLAIM_FILENAME);
	UT_ASSERT_EQ(unlink(claim), 0);
	UT_ASSERT_EQ(rmdir(generation), 0);
	UT_ASSERT_EQ(rename(moved, generation), 0);
}

static char v2_wal_segment_path[MAXPGPATH];
static char v2_wal_segment_moved[MAXPGPATH];

static void
v2_checkpoint_replace_wal_segment(void)
{
	uint8 *copy = malloc(wal_segment_size);
	test_checkpoint_x_hook = NULL;
	if (copy == NULL)
		abort();
	read_all_or_abort(v2_wal_segment_path, copy, wal_segment_size);
	copy[offsetof(XLogLongPageHeaderData, xlp_sysid)] ^= 1;
	if (rename(v2_wal_segment_path, v2_wal_segment_moved) != 0)
		abort();
	write_all_or_abort(v2_wal_segment_path, copy, wal_segment_size);
	free(copy);
}

UT_TEST(test_v2_checkpoint_rejects_replaced_wal_segment)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	v2_checkpoint_fixture(before, &self, &candidate);
	v2_checkpoint_wal_path(&self, candidate.checkPoint, candidate.checkPointCopy.ThisTimeLineID,
						   v2_wal_segment_path);
	snprintf(v2_wal_segment_moved, sizeof(v2_wal_segment_moved), "%s.moved", v2_wal_segment_path);
	test_checkpoint_x_hook = v2_checkpoint_replace_wal_segment;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	UT_ASSERT_EQ(test_walr_begin_calls, test_walr_end_calls);
	UT_ASSERT_EQ(test_cf_mode, NoLock);
	UT_ASSERT_EQ(unlink(v2_wal_segment_path), 0);
	UT_ASSERT_EQ(rename(v2_wal_segment_moved, v2_wal_segment_path), 0);
}

static uint8 v2_race_winner[66048];

static void
v2_checkpoint_root_race(void)
{
	char path[MAXPGPATH];

	test_checkpoint_x_hook = NULL;
	path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
	read_all_or_abort(path, v2_race_winner, sizeof(v2_race_winner));
	put_u64_le(v2_race_winner + 16, 8);
	put_u64_le(v2_race_winner + 336, 99);
	v2_checksums(v2_race_winner);
	v2_write_roots(v2_race_winner);
}

static void
v2_checkpoint_epoch_race(void)
{
	test_checkpoint_x_hook = NULL;
	test_epoch++;
}

UT_TEST(test_v2_checkpoint_cas_and_epoch_races_do_not_overwrite)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	v2_checkpoint_fixture(before, &self, &candidate);
	test_checkpoint_x_hook = v2_checkpoint_root_race;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(v2_race_winner);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	v2_assert_anchor_staging_empty();
	v2_checkpoint_fixture(before, &self, &candidate);
	test_checkpoint_x_hook = v2_checkpoint_epoch_race;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
}

UT_TEST(test_v2_checkpoint_boundaries_refuse_without_mutation)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	int fault;

	for (fault = 0; fault < 7; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		switch (fault) {
		case 0:
			put_u64_le(before + 16, UINT64_MAX);
			break;
		case 1:
			put_u64_le(before + 512 + 16, UINT64_MAX);
			break;
		case 2: {
			ClusterRecoveryAnchorV2 anchors[2];
			char path[MAXPGPATH];

			v2_thread_fixture(before, anchors);
			test_checkpoint_mode = true;
			anchors[0].anchor_generation = UINT64_MAX;
			put_u64_le(before + 512 + 256, UINT64_MAX);
			v2_anchor_object(before, &anchors[0], &anchors[0].identity, path);
			break;
		}
		case 3:
			put_u32_le(before + 196, 4);
			break;
		case 4:
			put_u64_le(before + 232, 0);
			break;
		case 5:
			test_walr_begin_result = CLUSTER_WAL_PIN_UNAVAILABLE;
			break;
		case 6:
			test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
			break;
		}
		v2_checksums(before);
		v2_write_roots(before);
		if (fault < 3)
			UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
						 CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED);
		else
			UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		v2_assert_primary_unchanged(before);
		v2_assert_anchor_staging_empty();
	}
}

UT_TEST(test_v2_checkpoint_root_io_failure_keeps_old_selection)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;

	v2_checkpoint_fixture(before, &self, &candidate);
	test_fail_primary_rename = true;
	UT_ASSERT_EQ(v2_checkpoint_publish(&self, &candidate, &out, &token),
				 CLUSTER_CONTROL_ROOT_IO_ERROR);
	UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
}

UT_TEST(test_v2_checkpoint_postwrite_failure_keeps_fact_but_no_success)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate, view;
	ControlRootImage root;
	int fault;

	for (fault = 0; fault < 3; ++fault) {
		v2_checkpoint_fixture(before, &self, &candidate);
		if (fault == 0)
			test_fence_after_primary = true;
		else if (fault == 1)
			test_release_after_primary = true;
		else
			test_walr_end_result = CLUSTER_WALR_RELEASE_UNCONFIRMED;
		UT_ASSERT(v2_checkpoint_publish(&self, &candidate, &out, &token) != 0);
		UT_ASSERT(v2_zero(&out, sizeof(out)) && v2_zero(&token, sizeof(token)));
		UT_ASSERT_EQ(cluster_control_root_v2_read_thread_locked(&self, &root, &view, &token), 0);
		UT_ASSERT_EQ(view.checkPoint, candidate.checkPoint);
		UT_ASSERT_EQ(token.file_txn_seq, 8);
		v2_assert_anchor_staging_empty();
	}
}

static void
v2_checkpoint_throw_on_x(void)
{
	test_checkpoint_x_hook = NULL;
	pg_re_throw();
}

UT_TEST(test_v2_checkpoint_error_unwind_releases_owned_work)
{
	uint8 before[66048];
	ClusterControlRootIdentity self;
	ClusterControlRootSnapshot out;
	ClusterControlRootFileToken token;
	ControlFileData candidate;
	volatile bool caught = false;
	int open_before = 0, open_after = 0;

	v2_checkpoint_fixture(before, &self, &candidate);
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_before++;
	test_checkpoint_x_hook = v2_checkpoint_throw_on_x;
	PG_TRY();
	{
		(void)v2_checkpoint_publish(&self, &candidate, &out, &token);
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught);
	UT_ASSERT_EQ(test_walr_begin_calls, 1);
	UT_ASSERT_EQ(test_walr_end_calls, 1);
	v2_assert_primary_unchanged(before);
	v2_assert_anchor_staging_empty();
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_after++;
	UT_ASSERT_EQ(open_after, open_before);
}

/* PGRAC: native CF entry consumes the real root/claim/anchor stack, not a
 * stubbed success. The compatibility image is valid but has a different
 * checkpoint so a fallback cannot accidentally satisfy these assertions.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
v2_runtime_fixture(uint8 bytes[66048], ClusterControlRootIdentity *self, ControlFileData *candidate)
{
	v2_checkpoint_fixture(bytes, self, candidate);
	cluster_shared_config = false;
	cluster_cf_authority_write(candidate);
	cluster_shared_config = true;
	test_cf_mode = ShareLock;
	test_checkpoint_outer_cf = true;
	MyAuxProcType = NotAnAuxProcess;
	test_epoch_reads = test_change_epoch_read = 0;
}

UT_TEST(test_v2_runtime_native_reader_selects_own_thread)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate, out;

	v2_runtime_fixture(bytes, &self, &candidate);
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.checkPoint, candidate.checkPoint - 8192);
	UT_ASSERT_EQ(out.checkPointCopy.nextOid, 60001);
	UT_ASSERT_EQ(test_cf_lock_calls, 0);
	cluster_node_id = 127;
	test_self_incarnation = test_membership_incarnation = 226;
	test_own_thread = 128;
	UT_ASSERT(cluster_cf_authority_read(&out));
	UT_ASSERT_EQ(out.checkPoint, UINT64_C(0x1000000) + 127 * 4096 + 128);
	UT_ASSERT_EQ(out.checkPointCopy.nextOid, 60001);
	cluster_shared_config = false;
}

UT_TEST(test_v2_runtime_reader_never_uses_projection_for_bad_facts)
{
	for (int fault = 0; fault < 16; ++fault) {
		uint8 bytes[66048];
		ClusterControlRootIdentity self;
		ControlFileData candidate, out;
		char path[MAXPGPATH];

		v2_runtime_fixture(bytes, &self, &candidate);
		switch (fault) {
		case 0:
			test_cf_clusterwide = false;
			break;
		case 1:
			test_self_incarnation++;
			break;
		case 2:
			test_membership_incarnation++;
			break;
		case 3:
			test_member_state = CLUSTER_MEMBER_DEAD;
			break;
		case 4:
			test_serving = false;
			break;
		case 5:
			test_fence = false;
			break;
		case 6:
			test_prebump = true;
			break;
		case 7:
			test_wal_validated = false;
			break;
		case 8:
			test_own_thread = 2;
			break;
		case 9:
			test_change_epoch_read = 3;
			break;
		case 10:
			v2_claim_path(&self, path);
			UT_ASSERT_EQ(unlink(path), 0);
			break;
		case 11:
			cluster_controlfile_shared_authority = false;
			break;
		case 12:
			put_u32_le(bytes + 196, CLUSTER_CONTROL_ROOT_DATABASE_CLOSED);
			break;
		case 13:
			put_u64_le(bytes + 232, 0);
			break;
		case 14:
			bytes[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_CLOSED;
			break;
		case 15:
			put_u32_le(bytes + 76, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
			break;
		}
		if (fault >= 12) {
			ControlRootImage root;

			v2_checksums(bytes);
			UT_ASSERT_EQ(
				cluster_control_root_v2_decode(bytes, sizeof(bytes), v2_storage, TEST_SYSID, &root),
				0);
			v2_write_roots(bytes);
		}
		out = candidate;
		UT_ASSERT(!cluster_cf_authority_read(&out));
		UT_ASSERT_EQ(memcmp(&out, &candidate, sizeof(out)), 0);
		/* Private decoder scratch clears on failure; native shared output is
		 * untouched, not treated as a successful fallback or new authority.
		 */
		test_epoch_reads = 0;
		if (fault == 9)
			--test_epoch;
		UT_ASSERT(cluster_control_root_v2_read_runtime_local_locked(&out)
				  != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
		UT_ASSERT(v2_zero(&out, sizeof(out)));
		cluster_shared_config = false;
	}
}

UT_TEST(test_v2_runtime_native_inplace_identity_is_never_cleared)
{
	uint8 bytes[66048];
	ClusterControlRootIdentity self;
	ControlFileData candidate, before;

	v2_runtime_fixture(bytes, &self, &candidate);
	/* Actual xlog.c shape: CF read's output is also GetSystemIdentifier's
	 * backing storage. It must never be zeroed while validating a new view.
	 */
	test_sysid_control = &candidate;
	UT_ASSERT(cluster_cf_authority_read(&candidate));
	UT_ASSERT_EQ(candidate.system_identifier, TEST_SYSID);
	UT_ASSERT_EQ(candidate.checkPoint, UINT64_C(0x1000000) + 128);
	before = candidate;
	test_fence = false;
	UT_ASSERT(!cluster_cf_authority_read(&candidate));
	UT_ASSERT_EQ(memcmp(&candidate, &before, sizeof(candidate)), 0);
	test_sysid_control = NULL;
	cluster_shared_config = false;
}

UT_TEST(test_v2_view_requires_exact_config_object)
{
	uint8 bytes[66048];
	ControlFileData native, out;
	ClusterCfImageStage stage;
	ControlRootImage root;
	ClusterControlRootFileToken token;
	char object[MAXPGPATH], path[MAXPGPATH], hex[65];

	for (int n = 0; n < 3; ++n) {
		v2_view_fixture(bytes, &native, &stage);
		for (int i = 0; i < 32; ++i)
			snprintf(hex + 2 * i, 3, "%02x", bytes[256 + i]);
		snprintf(object, sizeof(object), "%s/global/config_images/47-%s.conf", test_root, hex);
		if (n == 0)
			UT_ASSERT_EQ(unlink(object), 0);
		else if (n == 1)
			write_all_or_abort(object, (const uint8 *)"corrupt", 7);
		else {
			/* Even an exact hash cannot substitute a foreign identity. */
			v2_install_config(bytes, true);
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_REL_PATH);
			write_all_or_abort(path, bytes, sizeof(bytes));
			path_for(path, sizeof(path), CLUSTER_CONTROL_ROOT_BAK_REL_PATH);
			write_all_or_abort(path, bytes, sizeof(bytes));
		}
		memset(&root, 0xa5, sizeof(root));
		memset(&out, 0xa5, sizeof(out));
		memset(&token, 0xa5, sizeof(token));
		UT_ASSERT(
			cluster_control_root_v2_read_control_locked(v2_storage, TEST_SYSID, &root, &out, &token)
			!= 0);
		UT_ASSERT(v2_outputs_zero(&root, &out, &token));
	}
}

/* PGRAC: exact byte composition cannot turn a valid individual object into
 * startup permission. All input bytes below came from the real codecs/files.
 * Author: SqlRush <sqlrush@gmail.com>
 */
typedef struct BootstrapFixture {
	ClusterControlBootstrapInput input;
	uint8 binding[256];
	uint8 before[66048];
	uint8 after[66048];
	uint8 common[PG_CONTROL_FILE_SIZE];
	uint8 config[4096];
	uint8 claim[112];
	uint8 anchor[512];
	ClusterRecoveryAnchorV2 local_anchor;
	ClusterRecoveryAnchorV2 current_anchors[2];
} BootstrapFixture;

static void
bootstrap_hex(const uint8 hash[32], char hex[65])
{
	for (int i = 0; i < 32; i++)
		snprintf(hex + 2 * i, 3, "%02x", hash[i]);
}

static void
bootstrap_fixture(BootstrapFixture *f, int node)
{
	ClusterRecoveryAnchorV2 anchors[2];
	ControlRootImage root;
	PgracControlBinding binding;
	char path[MAXPGPATH], hex[65];
	struct stat st;

	memset(f, 0, sizeof(*f));
	v2_thread_fixture(f->before, anchors);
	/* This baseline has no retained writers; history fixtures install real
	 * selected objects rather than relying on the codec-only placeholder. */
	memset(f->before + 512 + 127 * 512 + 216, 0, 40);
	v2_checksums(f->before);
	v2_write_roots(f->before);
	memcpy(f->current_anchors, anchors, sizeof(anchors));
	if (cluster_control_root_v2_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root)
		!= 0)
		abort();
	f->local_anchor = anchors[node == 0 ? 0 : 1];
	memset(&binding, 0, sizeof(binding));
	binding.system_identifier = TEST_SYSID;
	memcpy(binding.storage_uuid, v2_storage, 16);
	memcpy(binding.authority_uuid, root.header.authority_uuid, 16);
	binding.database_incarnation = 41;
	binding.node_id = node;
	memset(binding.operation_uuid, 0x41, 16);
	memset(binding.source_cold_sha256, 0x42, 32);
	memset(binding.target_qualification_sha256, 0x43, 32);
	memcpy(binding.migration_round_sha256, root.header.migration_round_sha256, 32);
	memcpy(binding.source_wal_state_sha256, root.header.source_wal_state_sha256, 32);
	binding.migration_prepare_generation = 3;
	binding.migration_transition_epoch = 4;
	if (!pgrac_control_binding_encode(&binding, f->binding, sizeof(f->binding)))
		abort();

	bootstrap_hex(root.header.v2.control_image_sha256, hex);
	snprintf(path, sizeof(path), "%s/global/control_images/53-%s.bin", test_root, hex);
	read_all_or_abort(path, f->common, sizeof(f->common));
	bootstrap_hex(root.header.v2.config_sha256, hex);
	snprintf(path, sizeof(path), "%s/global/config_images/47-%s.conf", test_root, hex);
	if (stat(path, &st) != 0 || st.st_size <= 0 || st.st_size > sizeof(f->config))
		abort();
	read_all_or_abort(path, f->config, st.st_size);
	f->input.config.data = f->config;
	f->input.config.len = st.st_size;
	v2_claim_path(&f->local_anchor.identity, path);
	read_all_or_abort(path, f->claim, sizeof(f->claim));
	bootstrap_hex(root.refs[node].anchor_sha256, hex);
	snprintf(path, sizeof(path),
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, f->local_anchor.identity.origin_thread_id,
			 f->local_anchor.identity.origin_owner_incarnation, f->local_anchor.anchor_generation,
			 hex);
	read_all_or_abort(path, f->anchor, sizeof(f->anchor));
	memcpy(f->after, f->before, sizeof(f->after));
	f->input.node_id = node;
#define BOOT_INPUT(field, bytes)                                                                   \
	f->input.field.data = f->bytes;                                                                \
	f->input.field.len = sizeof(f->bytes)
	BOOT_INPUT(binding, binding);
	BOOT_INPUT(root_before, before);
	BOOT_INPUT(root_after, after);
	BOOT_INPUT(common, common);
	BOOT_INPUT(claim, claim);
	BOOT_INPUT(anchor, anchor);
#undef BOOT_INPUT
	/* Composition must not consult these unrelated runtime authorities. */
	test_cf_grant = false;
	test_cf_lock_calls = 0;
	test_contract = CLUSTER_CF_CONTRACT_UNVERIFIED;
}

static ClusterControlRootResult
bootstrap_refused(const ClusterControlBootstrapInput *input)
{
	ClusterControlBootstrapSnapshot out;
	ClusterControlRootResult result;

	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_bootstrap_decode(input, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	return result;
}

UT_TEST(test_bootstrap_composes_exact_threads_without_admission)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot out;
	uint8 hash[32];

	for (int node = 0; node <= 127; node += 127) {
		bootstrap_fixture(&f, node);
		UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &out), 0);
		UT_ASSERT_EQ(out.binding.node_id, node);
		UT_ASSERT_EQ(out.thread.origin_thread_id, node + 1);
		UT_ASSERT_EQ(out.thread.origin_owner_incarnation,
					 f.local_anchor.identity.origin_owner_incarnation);
		UT_ASSERT_EQ(out.control.checkPoint, f.local_anchor.checkpoint);
		UT_ASSERT_EQ(out.control.minRecoveryPoint, f.local_anchor.min_recovery_point);
		UT_ASSERT_EQ(out.control.MaxConnections, 300 + node);
		UT_ASSERT_EQ(out.control.checkPointCopy.nextOid, 60001);
		UT_ASSERT_EQ(out.config.identity.generation, 47);
		UT_ASSERT_EQ(out.root_sequence, 7);
		UT_ASSERT_EQ(out.database_state, CLUSTER_CONTROL_ROOT_DATABASE_OPEN);
		sha256_bytes(f.before, sizeof(f.before), hash);
		UT_ASSERT(memcmp(hash, out.root_sha256, 32) == 0);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
	}
}

UT_TEST(test_bootstrap_every_local_root_identity_must_match)
{
	BootstrapFixture f;
	PgracControlBinding binding;

	bootstrap_fixture(&f, 0);
	for (int fault = 0; fault < 9; fault++) {
		UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
		switch (fault) {
		case 0:
			binding.system_identifier++;
			break;
		case 1:
			binding.storage_uuid[0] ^= 1;
			break;
		case 2:
			binding.authority_uuid[0] ^= 1;
			break;
		case 3:
			binding.database_incarnation++;
			break;
		case 4:
			binding.node_id = 127;
			break;
		case 5:
			binding.migration_round_sha256[0] ^= 1;
			break;
		case 6:
			binding.source_wal_state_sha256[0] ^= 1;
			break;
		case 7:
			binding.migration_prepare_generation++;
			break;
		case 8:
			binding.migration_transition_epoch++;
			break;
		}
		UT_ASSERT(pgrac_control_binding_encode(&binding, f.binding, sizeof(f.binding)));
		UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
		bootstrap_fixture(&f, 0);
	}
}

UT_TEST(test_bootstrap_changed_root_is_not_a_partial_success)
{
	BootstrapFixture f;

	bootstrap_fixture(&f, 0);
	put_u64_le(f.after + 16, 8);
	v2_checksums(f.after);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	/* A revoked after-image is terminal, not a retry that could reopen it. */
	put_u32_le(f.after + 196, CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
	v2_checksums(f.after);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID);
	f.after[200] ^= 1;
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC);
}

UT_TEST(test_bootstrap_absent_unconfigured_retired_or_revoked)
{
	BootstrapFixture f;

	for (int fault = 0; fault < 4; fault++) {
		bootstrap_fixture(&f, 0);
		if (fault == 0)
			memset(f.before + 512, 0, 512);
		else if (fault == 1) {
			put_u64_le(f.before + 216, 0);
			put_u64_le(f.before + 232, 0);
		} else if (fault == 2)
			f.before[512 + 10] = CLUSTER_CONTROL_ROOT_LIFECYCLE_RETIRED;
		else
			put_u32_le(f.before + 196, CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
		v2_checksums(f.before);
		memcpy(f.after, f.before, sizeof(f.after));
		bootstrap_refused(&f.input);
	}
}

UT_TEST(test_bootstrap_every_selected_object_is_required)
{
	BootstrapFixture f;
	uint8 *objects[5];

	for (int fault = 0; fault < 5; fault++) {
		bootstrap_fixture(&f, 0);
		objects[0] = f.binding;
		objects[1] = f.common;
		objects[2] = f.config;
		objects[3] = f.claim;
		objects[4] = f.anchor;
		objects[fault][20] ^= 1;
		bootstrap_refused(&f.input);
	}
	bootstrap_fixture(&f, 0);
	put_u16_le(f.before + 4, 1);
	v2_checksums(f.before);
	memcpy(f.after, f.before, sizeof(f.after));
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_BAD_VERSION);
}

UT_TEST(test_bootstrap_bad_input_lengths_and_alias_clear_output)
{
	BootstrapFixture f;
	ClusterControlBootstrapBytes *objects[7];
	ClusterControlBootstrapSnapshot out;

	bootstrap_fixture(&f, 0);
	objects[0] = &f.input.binding;
	objects[1] = &f.input.root_before;
	objects[2] = &f.input.root_after;
	objects[3] = &f.input.common;
	objects[4] = &f.input.config;
	objects[5] = &f.input.claim;
	objects[6] = &f.input.anchor;
	UT_ASSERT_EQ(bootstrap_refused(NULL), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	for (int i = 0; i < 7; i++) {
		ClusterControlBootstrapBytes saved = *objects[i];

		objects[i]->data = NULL;
		bootstrap_refused(&f.input);
		*objects[i] = saved;
		objects[i]->len--;
		bootstrap_refused(&f.input);
		*objects[i] = saved;
		objects[i]->len = SIZE_MAX;
		bootstrap_refused(&f.input);
		*objects[i] = saved;
	}
	f.input.node_id = 128;
	bootstrap_refused(&f.input);
	f.input.node_id = 0;
	UT_ASSERT_EQ(
		cluster_control_bootstrap_decode(&f.input, (ClusterControlBootstrapSnapshot *)f.before),
		CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(f.before, sizeof(out)));
}

static void
bootstrap_replace_anchor(BootstrapFixture *f)
{
	uint8 hash[32];

	UT_ASSERT_EQ(cluster_recovery_anchor_v2_encode(&f->local_anchor, f->anchor), 0);
	sha256_bytes(f->anchor, sizeof(f->anchor), hash);
	memcpy(f->before + 512 + 264, hash, 32);
	v2_checksums(f->before);
	memcpy(f->after, f->before, sizeof(f->after));
}

UT_TEST(test_bootstrap_anchor_requires_exact_redo_and_no_backup)
{
	BootstrapFixture f;

	bootstrap_fixture(&f, 0);
	f.local_anchor.checkpoint_copy.redo += 4096;
	f.local_anchor.checkpoint += 4096;
	f.local_anchor.min_recovery_point += 4096;
	bootstrap_replace_anchor(&f);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	bootstrap_fixture(&f, 0);
	f.local_anchor.backup_start = UINT64_C(0x1000100);
	f.local_anchor.backup_end = UINT64_C(0x1000200);
	f.local_anchor.backup_end_required = false;
	bootstrap_replace_anchor(&f);
	UT_ASSERT_EQ(bootstrap_refused(&f.input), CLUSTER_CONTROL_ROOT_RANGE_INVALID);
}

UT_TEST(test_bootstrap_prepared_observation_is_not_open)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot out;

	for (int state = 1; state <= 5; state++) {
		bootstrap_fixture(&f, 0);
		put_u32_le(f.before + 76, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
		put_u32_le(f.before + 196, state);
		v2_checksums(f.before);
		memcpy(f.after, f.before, sizeof(f.after));
		UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &out), 0);
		UT_ASSERT_EQ(out.activation_state, CLUSTER_CONTROL_ROOT_ACTIVATION_PREPARED);
		UT_ASSERT_EQ(out.database_state, state);
		UT_ASSERT_EQ(out.control.state, DB_IN_PRODUCTION);
	}
}

/* PGRAC: real filesystem collection, with syscall-timed mutation only.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static char bootstrap_local[MAXPGPATH];
static int bootstrap_race;
static int bootstrap_root_opens;
static int bootstrap_race_at;
static int bootstrap_close_calls;
static int bootstrap_close_fail_at;
static bool bootstrap_wal_route_race;
static uint8 bootstrap_replacement[66048];
static uint8 bootstrap_binding_replacement[256];

int unit_bootstrap_openat(int dir, const char *name, int flags, ...);
int unit_bootstrap_close(int fd);

int
unit_bootstrap_close(int fd)
{
	int result = close(fd);

	if (++bootstrap_close_calls == bootstrap_close_fail_at) {
		errno = EIO;
		return -1;
	}
	return result;
}

int
unit_bootstrap_openat(int dir, const char *name, int flags, ...)
{
	char primary[MAXPGPATH], staging[MAXPGPATH], binding[MAXPGPATH];
	bool root_open = strcmp(name, "pgrac_control_root") == 0;
	bool missing_object = bootstrap_race == 2 && strstr(name, ".bin") != NULL;
	if (bootstrap_wal_route_race && strcmp(name, "pg_wal") == 0) {
		int fd = openat(dir, name, flags);
		bootstrap_wal_route_race = false;
		if (fd < 0 || unlinkat(dir, name, 0) != 0
			|| symlinkat(cluster_wal_threads_dir, dir, name) != 0)
			abort();
		return fd;
	}

	if (root_open)
		bootstrap_root_opens++;
	if ((root_open && bootstrap_root_opens == bootstrap_race_at && bootstrap_race != 0)
		|| missing_object) {
		path_for(primary, sizeof(primary), CLUSTER_CONTROL_ROOT_REL_PATH);
		path_for(staging, sizeof(staging), "global/bootstrap-test-new-root");
		if (bootstrap_race == 6) {
			path_for(primary, sizeof(primary), "global");
			path_for(staging, sizeof(staging), "global.bootstrap-saved");
			if (rename(primary, staging) != 0 || mkdir(primary, 0700) != 0)
				abort();
		} else if (bootstrap_race == 5) {
			snprintf(binding, sizeof(binding), "%s/global/%s", bootstrap_local,
					 PGRAC_CONTROL_BINDING_NAME);
			write_all_or_abort(binding, bootstrap_binding_replacement, 256);
		} else {
			write_all_or_abort(staging, bootstrap_replacement, 66048);
			if (rename(staging, primary) != 0)
				abort();
		}
		bootstrap_race = 0;
		if (missing_object) {
			/* Retiring an old object after publishing a new root is real I/O. */
			if (unlinkat(dir, name, 0) != 0)
				abort();
		}
	}
	return openat(dir, name, flags);
}

static void
bootstrap_read_fixture(BootstrapFixture *f, int node)
{
	char path[MAXPGPATH];

	bootstrap_race = bootstrap_root_opens = 0;
	bootstrap_race_at = 2;
	bootstrap_close_calls = bootstrap_close_fail_at = 0;
	bootstrap_fixture(f, node);
	if (bootstrap_local[0] == '\0') {
		strlcpy(bootstrap_local, "/tmp/pgrac-bootstrap-local.XXXXXX", sizeof(bootstrap_local));
		if (mkdtemp(bootstrap_local) == NULL)
			abort();
		snprintf(path, sizeof(path), "%s/global", bootstrap_local);
		if (mkdir(path, 0700) != 0)
			abort();
	}
	snprintf(path, sizeof(path), "%s/global/%s", bootstrap_local, PGRAC_CONTROL_BINDING_NAME);
	write_all_or_abort(path, f->binding, sizeof(f->binding));
}

UT_TEST(test_bootstrap_wal_route_exact_generation_and_refusals)
{
	BootstrapFixture f;
	ClusterControlBootstrapSnapshot snapshot;
	ClusterWalDurablePrefix prefix = { .sequence = 1 };
	char generation[MAXPGPATH], prefix_dir[MAXPGPATH], current[MAXPGPATH];
	char pgwal[MAXPGPATH], moved[MAXPGPATH];
	uint8 bytes[CLUSTER_WAL_DURABLE_PREFIX_BYTES];
	int before = 0, after = 0;

	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			before++;
	for (int fault = 0; fault < 11; fault++) {
		bootstrap_read_fixture(&f, fault == 0 ? 127 : 0);
		UT_ASSERT_EQ(cluster_control_bootstrap_decode(&f.input, &snapshot), 0);
		v2_claim_path(&snapshot.thread, generation);
		*strrchr(generation, '/') = '\0';
		snprintf(prefix_dir, sizeof(prefix_dir), "%s/durable_prefix", generation);
		UT_ASSERT(mkdir(prefix_dir, 0700) == 0 || errno == EEXIST);
		snprintf(current, sizeof(current), "%s/current", prefix_dir);
		UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&snapshot.wal, &prefix, bytes), 0);
		if (fault == 4)
			bytes[140] ^= 1;
		if (fault == 5) {
			ClusterWalDurablePrefixRef foreign = snapshot.wal;
			foreign.timeline++;
			UT_ASSERT_EQ(cluster_wal_durable_prefix_encode(&foreign, &prefix, bytes), 0);
		}
		write_all_or_abort(current, bytes, sizeof(bytes));
		snprintf(pgwal, sizeof(pgwal), "%s/pg_wal", bootstrap_local);
		UT_ASSERT(unlink(pgwal) == 0 || errno == ENOENT);
		UT_ASSERT_EQ(symlink(fault == 1 ? cluster_wal_threads_dir : generation, pgwal), 0);
		if (fault == 2)
			UT_ASSERT_EQ(unlink(pgwal), 0);
		if (fault == 3 || fault == 8)
			UT_ASSERT_EQ(unlink(current), 0);
		if (fault == 6)
			UT_ASSERT_EQ(chmod(generation, 0777), 0);
		if (fault == 7) {
			snprintf(moved, sizeof(moved), "%s.saved", generation);
			UT_ASSERT_EQ(rename(generation, moved), 0);
			UT_ASSERT_EQ(symlink(moved, generation), 0);
		}
		if (fault == 8)
			UT_ASSERT_EQ(mkfifo(current, 0600), 0);
		bootstrap_wal_route_race = fault == 9;
		bootstrap_close_fail_at = fault == 10 ? 1 : 0;
		if (fault == 0)
			UT_ASSERT_EQ(cluster_control_bootstrap_wal_route(
							 bootstrap_local, cluster_wal_threads_dir, &snapshot.wal),
						 0);
		else
			UT_ASSERT(cluster_control_bootstrap_wal_route(bootstrap_local, cluster_wal_threads_dir,
														  &snapshot.wal)
					  != 0);
		bootstrap_close_fail_at = 0;
		if (fault == 6)
			UT_ASSERT_EQ(chmod(generation, 0700), 0);
		if (fault == 7) {
			UT_ASSERT_EQ(unlink(generation), 0);
			UT_ASSERT_EQ(rename(moved, generation), 0);
		}
		if (fault == 8)
			UT_ASSERT_EQ(unlink(current), 0);
	}
	UT_ASSERT(cluster_control_bootstrap_wal_route(NULL, cluster_wal_threads_dir, &snapshot.wal)
			  != 0);
	UT_ASSERT(cluster_control_bootstrap_wal_route(bootstrap_local, cluster_wal_threads_dir, NULL)
			  != 0);
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			after++;
	UT_ASSERT_EQ(after, before);
}

static void
bootstrap_selected_path(const BootstrapFixture *f, int object, char path[MAXPGPATH])
{
	char hex[65];
	ControlRootImage root;
	uint32 node = f->input.node_id;

	if (cluster_control_root_v2_decode(f->before, sizeof(f->before), v2_storage, TEST_SYSID, &root))
		abort();
	switch (object) {
	case 0:
		bootstrap_hex(root.header.v2.control_image_sha256, hex);
		snprintf(path, MAXPGPATH, "%s/global/control_images/53-%s.bin", test_root, hex);
		break;
	case 1:
		bootstrap_hex(root.header.v2.config_sha256, hex);
		snprintf(path, MAXPGPATH, "%s/global/config_images/47-%s.conf", test_root, hex);
		break;
	case 2:
		v2_claim_path(&f->local_anchor.identity, path);
		break;
	case 3:
		bootstrap_hex(root.refs[node].anchor_sha256, hex);
		snprintf(path, MAXPGPATH,
				 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT
				 "/anchor_" UINT64_FORMAT "-%s.bin",
				 test_root, f->local_anchor.identity.origin_thread_id,
				 f->local_anchor.identity.origin_owner_incarnation,
				 f->local_anchor.anchor_generation, hex);
		break;
	case 4:
		path_for(path, MAXPGPATH, CLUSTER_CONTROL_ROOT_REL_PATH);
		break;
	case 5:
		snprintf(path, MAXPGPATH, "%s/global/%s", bootstrap_local, PGRAC_CONTROL_BINDING_NAME);
		break;
	default:
		abort();
	}
}

static ClusterControlRootResult
bootstrap_read_refused(uint32 node)
{
	ClusterControlBootstrapObservation out;
	ClusterControlRootResult result;

	memset(&out, 0xa5, sizeof(out));
	result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, node, &out);
	UT_ASSERT(result != CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY && out.config_bytes != NULL)
		pfree(out.config_bytes);
	return result;
}

UT_TEST(test_bootstrap_read_exact_files_and_owned_config)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;

	for (int node = 0; node <= 127; node += 127) {
		ClusterControlRootResult result;

		bootstrap_read_fixture(&f, node);
		result
			= cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, node, &out);
		UT_ASSERT_EQ(result, 0);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY || out.config_bytes == NULL)
			continue;
		UT_ASSERT_EQ(out.snapshot.thread.origin_thread_id, node + 1);
		UT_ASSERT_EQ(out.snapshot.control.checkPoint, f.local_anchor.checkpoint);
		UT_ASSERT_EQ(out.snapshot.control.MaxConnections, 300 + node);
		UT_ASSERT_EQ(out.config_len, f.input.config.len);
		UT_ASSERT(memcmp(out.config_bytes, f.config, out.config_len) == 0);
		UT_ASSERT_EQ(out.config_bytes[out.config_len], '\0');
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		UT_ASSERT_EQ(bootstrap_root_opens, 3);
		v2_assert_primary_unchanged(f.before);
		pfree(out.config_bytes);
	}
}

UT_TEST(test_bootstrap_read_requires_independent_binding_and_node)
{
	BootstrapFixture f;
	char path[MAXPGPATH];

	bootstrap_read_fixture(&f, 0);
	bootstrap_selected_path(&f, 5, path);
	UT_ASSERT_EQ(unlink(path), 0);
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_ABSENT);
	UT_ASSERT_EQ(bootstrap_root_opens, 0);
	bootstrap_read_fixture(&f, 0);
	UT_ASSERT_EQ(bootstrap_read_refused(127), CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	UT_ASSERT_EQ(bootstrap_root_opens, 0);
	UT_ASSERT_EQ(bootstrap_read_refused(128), CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
}

UT_TEST(test_bootstrap_read_never_falls_back_to_valid_bak)
{
	BootstrapFixture f;
	char path[MAXPGPATH];

	for (int fault = 0; fault < 3; fault++) {
		bootstrap_read_fixture(&f, 0);
		bootstrap_selected_path(&f, 4, path);
		if (fault == 0)
			UT_ASSERT_EQ(unlink(path), 0);
		else {
			if (fault == 1)
				f.before[200] ^= 1;
			else {
				put_u16_le(f.before + 4, 1);
				v2_checksums(f.before);
			}
			write_all_or_abort(path, f.before, sizeof(f.before));
		}
		UT_ASSERT_EQ(bootstrap_read_refused(0), fault == 0	 ? CLUSTER_CONTROL_ROOT_ABSENT
												: fault == 1 ? CLUSTER_CONTROL_ROOT_BAD_HEADER_CRC
															 : CLUSTER_CONTROL_ROOT_BAD_VERSION);
	}
}

UT_TEST(test_bootstrap_read_rejects_every_bad_selected_file)
{
	BootstrapFixture f;
	char path[MAXPGPATH];
	uint8 bytes[66049];
	struct stat st;

	for (int object = 0; object < 6; object++)
		for (int fault = 0; fault < 4; fault++) {
			bootstrap_read_fixture(&f, 0);
			bootstrap_selected_path(&f, object, path);
			UT_ASSERT_EQ(stat(path, &st), 0);
			read_all_or_abort(path, bytes, st.st_size);
			if (fault == 0)
				UT_ASSERT_EQ(unlink(path), 0);
			else if (fault == 1) {
				bytes[20] ^= 1;
				write_all_or_abort(path, bytes, st.st_size);
			} else if (fault == 2)
				UT_ASSERT_EQ(truncate(path, st.st_size - 1), 0);
			else {
				bytes[st.st_size] = 0;
				write_all_or_abort(path, bytes, st.st_size + 1);
			}
			bootstrap_read_refused(0);
		}
}

UT_TEST(test_bootstrap_read_unsafe_leaves_do_not_block_or_leak)
{
	BootstrapFixture f;
	char path[MAXPGPATH], saved[MAXPGPATH];
	int open_before = 0, open_after = 0;

	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_before++;
	for (int object = 0; object < 6; object++)
		for (int fault = 0; fault < 3; fault++) {
			bootstrap_read_fixture(&f, 0);
			bootstrap_selected_path(&f, object, path);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0666), 0);
			else {
				snprintf(saved, sizeof(saved), "%s.saved", path);
				UT_ASSERT_EQ(rename(path, saved), 0);
				if (fault == 1)
					UT_ASSERT_EQ(symlink(saved, path), 0);
				else
					UT_ASSERT_EQ(mkfifo(path, 0600), 0);
			}
			bootstrap_read_refused(0);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0600), 0);
			else {
				UT_ASSERT_EQ(unlink(path), 0);
				UT_ASSERT_EQ(rename(saved, path), 0);
			}
		}
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_after++;
	UT_ASSERT_EQ(open_after, open_before);
}

UT_TEST(test_bootstrap_read_unsafe_directories_are_refused)
{
	BootstrapFixture f;
	char path[MAXPGPATH], saved[MAXPGPATH];

	for (int component = 0; component < 9; component++)
		for (int fault = 0; fault < 2; fault++) {
			bootstrap_read_fixture(&f, 0);
			switch (component) {
			case 0:
				strlcpy(path, test_root, sizeof(path));
				break;
			case 1:
				path_for(path, sizeof(path), "global");
				break;
			case 2:
				path_for(path, sizeof(path), "global/control_images");
				break;
			case 3:
				path_for(path, sizeof(path), "global/config_images");
				break;
			case 4:
				path_for(path, sizeof(path), "global/anchor_images");
				break;
			case 5:
				path_for(path, sizeof(path), "global/anchor_images/thread_1");
				break;
			case 6:
				path_for(path, sizeof(path), "global/anchor_images/thread_1/generation_99");
				break;
			case 7:
				strlcpy(path, test_wal_root, sizeof(path));
				break;
			case 8:
				snprintf(path, sizeof(path), "%s/thread_1/generation_99", test_wal_root);
				break;
			}
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0777), 0);
			else {
				snprintf(saved, sizeof(saved), "%s.saved", path);
				UT_ASSERT_EQ(rename(path, saved), 0);
				UT_ASSERT_EQ(symlink(saved, path), 0);
			}
			bootstrap_read_refused(0);
			if (fault == 0)
				UT_ASSERT_EQ(chmod(path, 0700), 0);
			else {
				UT_ASSERT_EQ(unlink(path), 0);
				UT_ASSERT_EQ(rename(saved, path), 0);
			}
		}
}

UT_TEST(test_bootstrap_read_real_root_replacement_and_binding_races)
{
	BootstrapFixture f;
	PgracControlBinding binding;

	for (int race = 1; race <= 5; race++) {
		bootstrap_read_fixture(&f, 0);
		memcpy(bootstrap_replacement, f.before, sizeof(f.before));
		put_u64_le(bootstrap_replacement + 16, 8);
		if (race == 3)
			put_u32_le(bootstrap_replacement + 196,
					   CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
		else if (race == 4)
			put_u64_le(bootstrap_replacement + 200, 42);
		v2_checksums(bootstrap_replacement);
		UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
		binding.target_qualification_sha256[0] ^= 1;
		UT_ASSERT(pgrac_control_binding_encode(&binding, bootstrap_binding_replacement, 256));
		bootstrap_race = race;
		UT_ASSERT_EQ(bootstrap_read_refused(0), race == 3 ? CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID
												: race == 4 || race == 5
													? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
													: CLUSTER_CONTROL_ROOT_STALE_TOKEN);
		UT_ASSERT_EQ(bootstrap_race, 0);
		UT_ASSERT_EQ(bootstrap_root_opens, 2);
	}
}

UT_TEST(test_bootstrap_read_invalid_paths_outputs_and_alias)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	const char *bad[] = { NULL, "", ".", "/", "/tmp/", "/tmp//x", "/tmp/./x", "/tmp/../x" };

	bootstrap_read_fixture(&f, 0);
	for (int i = 0; i < lengthof(bad); i++)
		for (int field = 0; field < 3; field++) {
			const char *paths[3] = { bootstrap_local, test_root, test_wal_root };
			paths[field] = bad[i];
			memset(&out, 0xa5, sizeof(out));
			UT_ASSERT_EQ(cluster_control_bootstrap_read(paths[0], paths[1], paths[2], 0, &out),
						 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
			UT_ASSERT(v2_zero(&out, sizeof(out)));
		}
	UT_ASSERT_EQ(cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, NULL),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	memset(&out, 0, sizeof(out));
	strlcpy((char *)&out, bootstrap_local, sizeof(out));
	UT_ASSERT_EQ(cluster_control_bootstrap_read((char *)&out, test_root, test_wal_root, 0, &out),
				 CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT);
	UT_ASSERT(v2_zero(&out, sizeof(out)));
	UT_ASSERT_EQ(bootstrap_root_opens, 0);
}

UT_TEST(test_bootstrap_read_pinned_directory_is_not_replacement)
{
	BootstrapFixture f;
	char current[MAXPGPATH], saved[MAXPGPATH];

	bootstrap_read_fixture(&f, 0);
	bootstrap_race = 6;
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(bootstrap_race, 0);
	path_for(current, sizeof(current), "global");
	path_for(saved, sizeof(saved), "global.bootstrap-saved");
	/* The replacement is our exact newly created empty test directory. */
	UT_ASSERT_EQ(rmdir(current), 0);
	UT_ASSERT_EQ(rename(saved, current), 0);
}

static void bootstrap_history_files(BootstrapFixture *f, uint32 node, uint32 count,
									char paths[3][MAXPGPATH]);

UT_TEST(test_bootstrap_read_close_failure_never_returns_partial_success)
{
	BootstrapFixture f;
	char paths[3][MAXPGPATH];
	ClusterControlBootstrapObservation out;
	ClusterControlRootResult result;
	int closes, open_before = 0, open_after = 0;

	bootstrap_read_fixture(&f, 0);
	bootstrap_history_files(&f, 127, 2, paths);
	result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out);
	UT_ASSERT_EQ(result, 0);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return;
	closes = bootstrap_close_calls;
	UT_ASSERT(closes > 10);
	pfree(out.config_bytes);
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_before++;
	for (int i = 1; i <= closes; i++) {
		bootstrap_read_fixture(&f, 0);
		bootstrap_history_files(&f, 127, 2, paths);
		bootstrap_close_fail_at = i;
		UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_IO_ERROR);
		UT_ASSERT(bootstrap_close_calls >= i);
	}
	for (int fd = 0; fd < 256; fd++)
		if (fcntl(fd, F_GETFD) >= 0)
			open_after++;
	UT_ASSERT_EQ(open_after, open_before);
	bootstrap_close_fail_at = 0;
}

/* PGRAC: real selected current/history files exercise complete sizing input.
 * Author: SqlRush <sqlrush@gmail.com>
 */
static void
bootstrap_source_paths(const uint8 root_bytes[66048], uint32 node, char claim[MAXPGPATH],
					   char anchor[MAXPGPATH])
{
	ControlRootImage root;
	char hex[65];
	if (cluster_control_root_v2_decode(root_bytes, 66048, v2_storage, TEST_SYSID, &root))
		abort();
	v2_claim_path(&root.records[node].identity, claim);
	bootstrap_hex(root.refs[node].anchor_sha256, hex);
	snprintf(anchor, MAXPGPATH,
			 "%s/global/anchor_images/thread_%u/generation_" UINT64_FORMAT "/anchor_" UINT64_FORMAT
			 "-%s.bin",
			 test_root, node + 1, root.records[node].identity.origin_owner_incarnation,
			 root.refs[node].anchor_generation, hex);
}

static void
bootstrap_history_files(BootstrapFixture *f, uint32 node, uint32 count, char paths[3][MAXPGPATH])
{
	uint8 bytes[65604], source[66048];
	ControlRootImage root;
	char dir[MAXPGPATH], hex[65];
	size_t len = 64 + count * 512 + 4;

	memset(bytes, 0, sizeof(bytes));
	memcpy(bytes, "PGWH", 4);
	put_u16_le(bytes + 4, 1);
	put_u16_le(bytes + 6, 64);
	put_u32_le(bytes + 8, count);
	put_u32_le(bytes + 12, 512);
	put_u64_le(bytes + 16, count * 512);
	put_u64_le(bytes + 24, TEST_SYSID);
	memcpy(bytes + 32, f->before + 32, 32);
	for (uint32 i = 0; i < count; i++) {
		ClusterRecoveryAnchorV2 a = f->current_anchors[node == 0 ? 0 : 1];
		uint8 *record = source + 512 + node * 512;
		memcpy(source, f->before, sizeof(source));
		put_u64_le(record + 80, 1000 + i);
		put_u64_le(record + 24, 2000 + i);
		put_u64_le(record + 72, 3000 + i);
		memset(record + 216, 0, 40);
		record[10] = 1 + i % 5;
		v2_checksums(source);
		if (cluster_control_root_v2_decode(source, sizeof(source), v2_storage, TEST_SYSID, &root))
			abort();
		v2_claim_object(source, node, &root);
		a.identity = root.records[node].identity;
		memcpy(a.claim_sha256, root.refs[node].claim_sha256, 32);
		a.max_connections = 600 + i;
		a.max_worker_processes = 20 + i;
		a.max_wal_senders = 8 + i;
		a.max_prepared_xacts = i;
		a.max_locks_per_xact = 100 + i;
		/* Modes are not quantities to fold into a maximum. */
		a.wal_level = i % 3;
		a.wal_log_hints = (i % 2) != 0;
		a.track_commit_timestamp = !a.wal_log_hints;
		v2_anchor_object(source, &a, &a.identity, paths[2]);
		bootstrap_source_paths(source, node, paths[1], paths[2]);
		memcpy(bytes + 64 + i * 512, record, 512);
	}
	put_u32_le(bytes + len - 4, image_crc(bytes, len - 4));
	put_u64_le(f->before + 512 + node * 512 + 216, 123);
	sha256_bytes(bytes, len, f->before + 512 + node * 512 + 224);
	bootstrap_hex(f->before + 512 + node * 512 + 224, hex);
	path_for(dir, sizeof(dir), "global/wal_history");
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(dir, sizeof(dir), "%s/global/wal_history/thread_%u", test_root, node + 1);
	UT_ASSERT(mkdir(dir, 0700) == 0 || errno == EEXIST);
	snprintf(paths[0], MAXPGPATH, "%s/history_123-%s.bin", dir, hex);
	write_all_or_abort(paths[0], bytes, len);
	v2_checksums(f->before);
	v2_write_roots(f->before);
}

UT_TEST(test_bootstrap_capacity_includes_every_current_and_retained_source)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	const uint32 counts[] = { 0, 2, 128 };
	char paths[3][MAXPGPATH];

	for (size_t i = 0; i < lengthof(counts); i++) {
		ClusterControlRootResult result;
		bootstrap_read_fixture(&f, 0);
		bootstrap_history_files(&f, 127, counts[i], paths);
		/* An unreferenced decoy may be broken and must not be enumerated. */
		path_for(paths[0], MAXPGPATH, "global/wal_history/thread_128/history_999-decoy.bin");
		write_all_or_abort(paths[0], (const uint8 *)"bad", 3);
		result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out);
		UT_ASSERT_EQ(result, 0);
		if (result != 0)
			continue;
		UT_ASSERT_EQ(out.required.current_sources, 2);
		UT_ASSERT_EQ(out.required.history_sources, counts[i]);
		UT_ASSERT_EQ(out.required.max_connections, counts[i] == 0 ? 427 : 599 + counts[i]);
		UT_ASSERT_EQ(out.required.max_worker_processes, counts[i] == 0 ? 16 : 19 + counts[i]);
		UT_ASSERT_EQ(out.required.max_wal_senders, counts[i] == 0 ? 5 : 7 + counts[i]);
		UT_ASSERT_EQ(out.required.max_prepared_xacts, counts[i] == 0 ? 0 : counts[i] - 1);
		UT_ASSERT_EQ(out.required.max_locks_per_xact, counts[i] == 0 ? 64 : 99 + counts[i]);
		UT_ASSERT_EQ(out.snapshot.control.MaxConnections, 300);
		UT_ASSERT_EQ(out.snapshot.control.wal_level, 1);
		UT_ASSERT_EQ(test_cf_lock_calls, 0);
		UT_ASSERT_EQ(test_durable_rename_calls, 0);
		v2_assert_primary_unchanged(f.before);
		pfree(out.config_bytes);
	}
}

UT_TEST(test_bootstrap_capacity_does_not_skip_retired_or_unconfigured_origin)
{
	BootstrapFixture f;
	ClusterControlBootstrapObservation out;
	for (int state = 1; state <= 5; state++) {
		ClusterControlRootResult result;
		bootstrap_read_fixture(&f, 0);
		f.before[512 + 127 * 512 + 10] = state;
		put_u64_le(f.before + 224, 0); /* configured excludes remote origin */
		put_u64_le(f.before + 240, 0); /* serving excludes it too */
		v2_checksums(f.before);
		v2_install_config(f.before, false);
		v2_write_roots(f.before);
		result = cluster_control_bootstrap_read(bootstrap_local, test_root, test_wal_root, 0, &out);
		UT_ASSERT_EQ(result, 0);
		if (result == 0) {
			UT_ASSERT_EQ(out.required.current_sources, 2);
			UT_ASSERT_EQ(out.required.max_connections, 427);
			pfree(out.config_bytes);
		}
	}
}

UT_TEST(test_bootstrap_capacity_requires_nonlocal_and_history_objects)
{
	BootstrapFixture f;
	char paths[3][MAXPGPATH], saved[MAXPGPATH];
	for (int retained = 0; retained <= 1; retained++)
		for (int object = retained ? 0 : 1; object < 3; object++)
			for (int fault = 0; fault < 5; fault++) {
				bootstrap_read_fixture(&f, 0);
				if (retained)
					bootstrap_history_files(&f, 127, 2, paths);
				else
					bootstrap_source_paths(f.before, 127, paths[1], paths[2]);
				if (fault == 0)
					UT_ASSERT_EQ(unlink(paths[object]), 0);
				else if (fault == 1)
					write_all_or_abort(paths[object], (const uint8 *)"bad", 3);
				else if (fault == 2)
					UT_ASSERT_EQ(chmod(paths[object], 0666), 0);
				else {
					snprintf(saved, sizeof(saved), "%s.saved", paths[object]);
					UT_ASSERT_EQ(rename(paths[object], saved), 0);
					if (fault == 3)
						UT_ASSERT_EQ(symlink(saved, paths[object]), 0);
					else
						UT_ASSERT_EQ(mkfifo(paths[object], 0600), 0);
				}
				bootstrap_read_refused(0);
				if (fault == 2)
					UT_ASSERT_EQ(chmod(paths[object], 0600), 0);
				if (fault >= 3) {
					UT_ASSERT_EQ(unlink(paths[object]), 0);
					UT_ASSERT_EQ(rename(saved, paths[object]), 0);
				}
			}
}

UT_TEST(test_bootstrap_capacity_reobserves_after_collection)
{
	BootstrapFixture f;
	PgracControlBinding binding;
	char paths[3][MAXPGPATH];
	for (int object_failure = 0; object_failure <= 1; object_failure++)
		for (int race = 1; race <= 5; race++) {
			if (race == 2)
				continue; /* earlier missing-object hook already tested */
			bootstrap_read_fixture(&f, 0);
			bootstrap_history_files(&f, 127, 2, paths);
			if (object_failure)
				UT_ASSERT_EQ(unlink(paths[2]), 0);
			memcpy(bootstrap_replacement, f.before, sizeof(f.before));
			put_u64_le(bootstrap_replacement + 16, 8);
			if (race == 3)
				put_u32_le(bootstrap_replacement + 196,
						   CLUSTER_CONTROL_ROOT_DATABASE_MIGRATION_REVOKED);
			else if (race == 4)
				put_u64_le(bootstrap_replacement + 200, 42);
			v2_checksums(bootstrap_replacement);
			UT_ASSERT(pgrac_control_binding_decode(f.binding, sizeof(f.binding), &binding));
			binding.target_qualification_sha256[0] ^= 1;
			UT_ASSERT(pgrac_control_binding_encode(&binding, bootstrap_binding_replacement, 256));
			bootstrap_race = race;
			bootstrap_race_at = 3;
			UT_ASSERT_EQ(bootstrap_read_refused(0),
						 race == 3				  ? CLUSTER_CONTROL_ROOT_LIFECYCLE_INVALID
						 : race == 4 || race == 5 ? CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH
												  : CLUSTER_CONTROL_ROOT_STALE_TOKEN);
			UT_ASSERT_EQ(bootstrap_race, 0);
			UT_ASSERT_EQ(bootstrap_root_opens, 3);
		}
}

UT_TEST(test_bootstrap_capacity_checks_nonlocal_content_not_just_file_presence)
{
	BootstrapFixture f;
	char path[MAXPGPATH], paths[3][MAXPGPATH];
	for (int fault = 0; fault < 8; fault++) {
		ClusterRecoveryAnchorV2 anchor;
		bootstrap_read_fixture(&f, 0);
		anchor = f.current_anchors[1];
		switch (fault) {
		case 0:
			anchor.checkpoint_copy.redo++;
			break;
		case 1:
			anchor.checkpoint_copy.ThisTimeLineID++;
			break;
		case 2:
			anchor.backup_start = 1;
			break;
		case 3:
			anchor.backup_end = 1;
			break;
		case 4:
			/* The production encoder already refuses this unsupported input.
			 * Mutate encoded bytes below to exercise the consumer as well. */
			break;
		case 5:
			anchor.identity.system_identifier++;
			break;
		case 6:
			anchor.identity.origin_owner_incarnation++;
			break;
		case 7:
			anchor.claim_sha256[0] ^= 1;
			break;
		}
		v2_anchor_object(f.before, &anchor, &f.current_anchors[1].identity, path);
		if (fault == 4) {
			uint8 bytes[512];
			read_all_or_abort(path, bytes, sizeof(bytes));
			bytes[288] = 1;
			put_u32_le(bytes + 508, image_crc(bytes, 508));
			sha256_bytes(bytes, sizeof(bytes), f.before + 512 + 127 * 512 + 264);
			v2_checksums(f.before);
			bootstrap_source_paths(f.before, 127, paths[1], path);
			write_all_or_abort(path, bytes, sizeof(bytes));
		}
		v2_write_roots(f.before);
		UT_ASSERT_EQ(bootstrap_read_refused(0), fault >= 2 && fault <= 4
													? CLUSTER_CONTROL_ROOT_RANGE_INVALID
													: CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH);
	}
	for (int object = 0; object < 3; object++) {
		uint8 bytes[1092];
		size_t len = object == 0 ? sizeof(bytes) : object == 1 ? 112 : 512;
		bootstrap_read_fixture(&f, 0);
		bootstrap_history_files(&f, 127, 2, paths);
		read_all_or_abort(paths[object], bytes, len);
		bytes[40] ^= 1;
		write_all_or_abort(paths[object], bytes, len);
		bootstrap_read_refused(0);
	}
	bootstrap_read_fixture(&f, 0);
	bootstrap_source_paths(f.before, 127, paths[1], paths[2]);
	/* A native v1-length claim is not auto-upgraded using current identity. */
	write_all_or_abort(paths[1], f.claim, 40);
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_BAD_SIZE);
}

UT_TEST(test_bootstrap_capacity_final_directory_replacement_is_not_pinned_success)
{
	BootstrapFixture f;
	char saved[MAXPGPATH], path[MAXPGPATH];
	bootstrap_read_fixture(&f, 0);
	bootstrap_race = 6;
	bootstrap_race_at = 3;
	UT_ASSERT_EQ(bootstrap_read_refused(0), CLUSTER_CONTROL_ROOT_STALE_TOKEN);
	UT_ASSERT_EQ(bootstrap_race, 0);
	path_for(path, sizeof(path), "global");
	path_for(saved, sizeof(saved), "global.bootstrap-saved");
	UT_ASSERT_EQ(rmdir(path), 0);
	UT_ASSERT_EQ(rename(saved, path), 0);
}

int
main(int argc, char **argv)
{
	if (argc > 1 && strcmp(argv[1], "--fixture-root-cast") == 0)
		return fixture_cast_main(argc, argv);
	if (argc > 1)
		return fixture_root_main(argc, argv);
	setup_fixture();

	UT_PLAN(110);
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
	UT_RUN(test_history_exact_empty_and_full_set_preserves_inputs);
	UT_RUN(test_history_rejects_outer_shape_crc_hash_and_identity);
	UT_RUN(test_history_rejects_bad_records_and_namespace_aliases);
	UT_RUN(test_history_refuses_unselected_and_invalid_arguments);
	UT_RUN(test_history_refuses_alias_before_clearing_output);
	UT_RUN(test_v2_decodes_exact_common_and_two_thread_fields);
	UT_RUN(test_v2_encoder_preserves_exact_bytes_and_publishers);
	UT_RUN(test_v2_versions_are_independent_and_strict);
	UT_RUN(test_v2_checks_all_three_crc_layers);
	UT_RUN(test_v2_reserved_bytes_and_partial_holes_are_rejected);
	UT_RUN(test_v2_requires_object_references_and_bound_membership);
	UT_RUN(test_v2_identity_and_node_thread_cannot_be_substituted);
	UT_RUN(test_v2_database_state_validation_is_not_an_open_decision);
	UT_RUN(test_v2_bad_arguments_and_size_clear_output);
	UT_RUN(test_v2_encoder_refuses_bad_logical_fields_without_bytes);
	UT_RUN(test_v2_max_generations_remain_readable_without_advancement);
	UT_RUN(test_v1_io_does_not_silently_consume_or_convert_v2);
	UT_RUN(test_v2_view_selects_exact_hash_not_decoy_or_projection);
	UT_RUN(test_v2_view_requires_clusterwide_lock_and_verified_storage);
	UT_RUN(test_v2_view_valid_backup_never_substitutes_for_current);
	UT_RUN(test_v2_view_backup_divergence_and_degraded_are_distinct);
	UT_RUN(test_v2_view_selected_object_failure_clears_valid_root);
	UT_RUN(test_v2_view_token_covers_whole_root_not_only_control_hash);
	UT_RUN(test_v2_view_rejects_v1_and_invalid_input_without_conversion);
	UT_RUN(test_v2_view_single_node_does_not_bypass_shared_storage_qualification);
	UT_RUN(test_v2_thread_view_selects_each_exact_thread);
	UT_RUN(test_v2_thread_view_rejects_stale_caller_and_absent_record);
	UT_RUN(test_v2_thread_view_clears_root_after_missing_anchor);
	UT_RUN(test_v2_thread_view_rejects_selected_foreign_or_inconsistent_anchor);
	UT_RUN(test_v2_checkpoint_advances_one_thread_and_preserves_common);
	UT_RUN(test_v2_checkpoint_requires_actual_wal_record);
	UT_RUN(test_v2_checkpoint_requires_exact_durable_prefix);
	UT_RUN(test_v2_checkpoint_rechecks_durable_prefix);
	UT_RUN(test_v2_checkpoint_wal_continuation);
	UT_RUN(test_v2_checkpoint_rejects_replaced_wal_directory);
	UT_RUN(test_v2_checkpoint_rejects_replaced_wal_segment);
	UT_RUN(test_v2_checkpoint_preserves_historical_parameter_requirements);
	UT_RUN(test_v2_checkpoint_rejects_invalid_parameter_before_max);
	UT_RUN(test_v2_checkpoint_cannot_invent_parameter_transition_proof);
	UT_RUN(test_v2_checkpoint_rejects_non_owner_facts_before_io);
	UT_RUN(test_v2_checkpoint_rejects_unflushed_wrong_tli_and_bad_inputs);
	UT_RUN(test_v2_checkpoint_cas_and_epoch_races_do_not_overwrite);
	UT_RUN(test_v2_checkpoint_boundaries_refuse_without_mutation);
	UT_RUN(test_v2_checkpoint_root_io_failure_keeps_old_selection);
	UT_RUN(test_v2_checkpoint_postwrite_failure_keeps_fact_but_no_success);
	UT_RUN(test_v2_checkpoint_error_unwind_releases_owned_work);
	UT_RUN(test_v2_thread_view_requires_physical_selected_claim);
	UT_RUN(test_v2_runtime_native_reader_selects_own_thread);
	UT_RUN(test_v2_runtime_reader_never_uses_projection_for_bad_facts);
	UT_RUN(test_v2_runtime_native_inplace_identity_is_never_cleared);
	UT_RUN(test_v2_view_requires_exact_config_object);
	UT_RUN(test_bootstrap_composes_exact_threads_without_admission);
	UT_RUN(test_bootstrap_every_local_root_identity_must_match);
	UT_RUN(test_bootstrap_changed_root_is_not_a_partial_success);
	UT_RUN(test_bootstrap_absent_unconfigured_retired_or_revoked);
	UT_RUN(test_bootstrap_every_selected_object_is_required);
	UT_RUN(test_bootstrap_bad_input_lengths_and_alias_clear_output);
	UT_RUN(test_bootstrap_anchor_requires_exact_redo_and_no_backup);
	UT_RUN(test_bootstrap_prepared_observation_is_not_open);
	UT_RUN(test_bootstrap_read_exact_files_and_owned_config);
	UT_RUN(test_bootstrap_read_requires_independent_binding_and_node);
	UT_RUN(test_bootstrap_read_never_falls_back_to_valid_bak);
	UT_RUN(test_bootstrap_read_rejects_every_bad_selected_file);
	UT_RUN(test_bootstrap_read_unsafe_leaves_do_not_block_or_leak);
	UT_RUN(test_bootstrap_wal_route_exact_generation_and_refusals);
	UT_RUN(test_bootstrap_read_unsafe_directories_are_refused);
	UT_RUN(test_bootstrap_read_real_root_replacement_and_binding_races);
	UT_RUN(test_bootstrap_read_invalid_paths_outputs_and_alias);
	UT_RUN(test_bootstrap_read_pinned_directory_is_not_replacement);
	UT_RUN(test_bootstrap_read_close_failure_never_returns_partial_success);
	UT_RUN(test_bootstrap_capacity_includes_every_current_and_retained_source);
	UT_RUN(test_bootstrap_capacity_does_not_skip_retired_or_unconfigured_origin);
	UT_RUN(test_bootstrap_capacity_requires_nonlocal_and_history_objects);
	UT_RUN(test_bootstrap_capacity_reobserves_after_collection);
	UT_RUN(test_bootstrap_capacity_checks_nonlocal_content_not_just_file_presence);
	UT_RUN(test_bootstrap_capacity_final_directory_replacement_is_not_pinned_success);
	UT_DONE();

	return ut_failed_count == 0 ? 0 : 1;
}
