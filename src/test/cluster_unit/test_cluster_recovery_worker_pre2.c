/* PGRAC: actual worker consumers and selected-generation filesystem reads.
 * Canonical ROOT/source owners are boundary doubles; claim/WAL reads are real.
 * Author: SqlRush <sqlrush@gmail.com> */
#define USE_PGRAC_CLUSTER 1
#include "postgres.h"
#include <sys/stat.h>
#include "common/cryptohash.h"
#include "common/sha2.h"
#include "utils/memutils.h"
#include "utils/resowner_private.h"
#include "cluster/cluster_wal_source.h"
#include "../../backend/cluster/cluster_control_root_private.h"
#include "unit_test.h"
UT_DEFINE_GLOBALS();

#undef ereport
#define ereport(level, ...) do { (void)(level); (void)(__VA_ARGS__); } while (0)
#include "../../backend/cluster/cluster_recovery_worker.c"

bool cluster_shared_config = true;
bool cluster_enabled = true;
int cluster_node_id;
int cluster_recovery_workers_max = 1;
int cluster_recovery_stale_active_ms = 1000;
int CheckPointTimeout = 300;
int wal_segment_size = 1024 * 1024;
char *cluster_wal_threads_dir;
static uint32 wait_info;
uint32 *my_wait_event_info = &wait_info;
static bool latch;
static unsigned registry_reads, root_reads, source_reads;
static ClusterControlRootSnapshot root;
static ClusterControlRootReadToken token;
static ClusterWalSourceRef source;
static ClusterControlRootResult root_result, source_result;
static ClusterRecoveryWorkerPool worker_pool;
static unsigned legacy_pins, exit_callbacks;
static char scratch[MAXPGPATH], generation[MAXPGPATH], claim_path[MAXPGPATH], wal_path[MAXPGPATH];

int errmsg(const char *fmt, ...) { (void)fmt; return 0; }
int errhint(const char *fmt, ...) { (void)fmt; return 0; }
TimestampTz GetCurrentTimestamp(void) { return INT64CONST(1000000000); }
uint16 cluster_wal_thread_id(void) { return 1; }
ClusterRecoveryWorkerPool *cluster_recovery_worker_pool_ptr(void) { return &worker_pool; }
static char owner_token;
ResourceOwner CurrentResourceOwner;
MemoryContext TopMemoryContext;
static unsigned owner_creates, crypto_owned, crypto_unowned, crypto_handles;
void CreateAuxProcessResourceOwner(void)
{
	/* Allocation can ERROR: the claimed slot must already have exit cleanup. */
	UT_ASSERT_EQ(exit_callbacks, 1);
	UT_ASSERT_NULL(CurrentResourceOwner);
	CurrentResourceOwner = (ResourceOwner)&owner_token;
	owner_creates++;
}
void BackgroundWorkerUnblockSignals(void)
{
	UT_ASSERT_NOT_NULL(CurrentResourceOwner);
}
void *MemoryContextAlloc(MemoryContext context, Size size)
{ (void)context; return malloc(size); }
/* Link the backend hash provider, retaining its real resource-owner calls.
 * These are bounded owner bookkeeping doubles, not a replacement digest. */
void ResourceOwnerEnlargeCryptoHash(ResourceOwner owner)
{
	if (owner == NULL) crypto_unowned++; else crypto_owned++;
	UT_ASSERT_NOT_NULL(owner);
}
void ResourceOwnerRememberCryptoHash(ResourceOwner owner, Datum handle)
{ UT_ASSERT_NOT_NULL(owner); UT_ASSERT_NE(handle, 0); crypto_handles++; }
void ResourceOwnerForgetCryptoHash(ResourceOwner owner, Datum handle)
{ UT_ASSERT_NOT_NULL(owner); UT_ASSERT_NE(handle, 0); crypto_handles--; }
void before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{ (void)callback; (void)arg; exit_callbacks++; }
bool RegisterDynamicBackgroundWorker(BackgroundWorker *worker, BackgroundWorkerHandle **handle)
{ (void)worker; *handle = NULL; return true; }
bool cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out)
{
	memset(out, 0, sizeof(*out)); out->generated = true;
	out->own_thread = 1; out->n_crashed_candidate = 1; out->candidate_bitmap[0] = 2;
	return true;
}
bool cluster_thread_recovery_pin_projection(uint16 tid, uint64 epoch)
{ (void)tid; (void)epoch; legacy_pins++; return false; }
bool cluster_thread_recovery_projection_current(uint16 tid, uint64 epoch,
	ClusterControlRootReadToken *t, uint64 *tail, uint64 *lower, uint64 *life,
	uint32 *tail_tli, uint32 *checkpoint_tli)
{
	(void)tid; (void)epoch; (void)t; (void)tail; (void)lower; (void)life;
	(void)tail_tli; (void)checkpoint_tli; legacy_pins++; return false;
}
int BasicOpenFile(const char *path, int flags) { return open(path, flags, 0600); }
bool cluster_r4_bit22_cutover_active(void) { return latch; }
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 tid, ClusterWalStateSlot *out)
{
	(void)tid; memset(out, 0, sizeof(*out)); registry_reads++;
	return CLUSTER_WAL_SLOT_EMPTY;
}
ClusterControlRootResult
cluster_control_root_read_canonical_discovered(uint16 tid, ClusterControlRootSnapshot *out,
											 ClusterControlRootReadToken *out_token)
{
	UT_ASSERT_EQ(tid, 2); root_reads++; *out = root; *out_token = token;
	return root_result;
}
ClusterControlRootResult
cluster_control_root_recovery_source_v1(const ClusterControlRootSnapshot *expected,
	const ClusterControlRootReadToken *expected_token, ClusterWalSourceRef *out, XLogRecPtr *redo)
{
	source_reads++;
	UT_ASSERT(memcmp(expected, &root, sizeof(root)) == 0);
	UT_ASSERT(memcmp(expected_token, &token, sizeof(token)) == 0);
	if (expected->lifecycle != CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	*out = source; *redo = root.checkpoint_lower_lsn;
	return source_result;
}

static void
write_exact(const char *path, const void *bytes, size_t n, off_t size)
{
	int fd = open(path, O_CREAT | O_TRUNC | O_WRONLY, 0600);
	if (fd < 0 || write(fd, bytes, n) != n || ftruncate(fd, size) != 0 || close(fd) != 0)
		abort();
}

static void
fixture(void)
{
	ClusterWalThreadClaimV2 claim = { 0 };
	uint8 bytes[CLUSTER_WAL_CLAIM_V2_BYTES];
	pg_cryptohash_ctx *hash;
	char name[MAXFNAMELEN], pages[2 * XLOG_BLCKSZ];
	uint64 start = 2 * (uint64)wal_segment_size;

	/* The launching Startup has an owner; the spawned SHMEM-only worker does
	 * not inherit that process-local lifetime. */
	CurrentResourceOwner = (ResourceOwner)&owner_token;
	owner_creates = crypto_owned = crypto_unowned = crypto_handles = 0;
	if (scratch[0] && !rmtree(scratch, true)) abort();
	strlcpy(scratch, "/tmp/pgrac-worker-pre2-XXXXXX", sizeof(scratch));
	if (!mkdtemp(scratch)) abort();
	snprintf(generation, sizeof(generation), "%s/thread_2", scratch);
	if (mkdir(generation, 0700)) abort();
	snprintf(generation, sizeof(generation), "%s/thread_2/generation_99", scratch);
	if (mkdir(generation, 0700)) abort();
	cluster_wal_threads_dir = scratch;
	claim.identity.system_identifier = 9;
	claim.identity.storage_uuid[0] = 1;
	claim.identity.authority_uuid[6] = 0x40;
	claim.identity.authority_uuid[8] = 0x80;
	claim.identity.origin_thread_id = 2; claim.identity.origin_node_id = 1;
	claim.identity.origin_owner_incarnation = 99;
	claim.identity.root_lineage_seq = 3;
	claim.identity.thread_claim_created_at = 42;
	claim.database_incarnation = 7;
	claim.config_generation = claim.claim_generation = 1;
	if (cluster_wal_claim_v2_encode(&claim, bytes) != CLUSTER_CONTROL_ROOT_OK_PRIMARY) abort();
	memset(&source, 0, sizeof(source));
	source.claim.identity = claim.identity;
	for (unsigned i = 0; i < 4; i++)
		source.claim.identity.thread_claim_crc32c |= (uint32)bytes[104 + i] << (8 * i);
	source.claim.database_incarnation = 7; source.claim.max_config_generation = 1;
	source.timeline = 1;
	hash = pg_cryptohash_create(PG_SHA256);
	if (!hash || pg_cryptohash_init(hash) || pg_cryptohash_update(hash, bytes, sizeof(bytes))
		|| pg_cryptohash_final(hash, source.claim.claim_sha256, 32)) abort();
	pg_cryptohash_free(hash);
	snprintf(claim_path, sizeof(claim_path), "%s/pgrac_thread.claim", generation);
	write_exact(claim_path, bytes, sizeof(bytes), sizeof(bytes));
	memset(pages, 0, sizeof(pages));
	for (unsigned i = 0; i < 2; i++) {
		XLogPageHeader hdr = (XLogPageHeader)(pages + i * XLOG_BLCKSZ);
		hdr->xlp_magic = XLOG_PAGE_MAGIC; hdr->xlp_tli = 1;
		hdr->xlp_pageaddr = start + i * XLOG_BLCKSZ; hdr->xlp_thread_id = 2;
	}
	XLogFileName(name, 1, 2, wal_segment_size);
	snprintf(wal_path, sizeof(wal_path), "%s/%s", generation, name);
	write_exact(wal_path, pages, sizeof(pages), wal_segment_size);
	memset(&root, 0, sizeof(root)); memset(&token, 0, sizeof(token));
	root.identity = source.claim.identity;
	root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_RECOVERY_REQUIRED;
	root.root_flags = CLUSTER_CONTROL_ROOT_FLAG_CLAIM_VALID
		| CLUSTER_CONTROL_ROOT_FLAG_CHECKPOINT_VALID | CLUSTER_CONTROL_ROOT_FLAG_TAIL_VALID;
	root.checkpoint_tli = root.tail_tli = 1;
	root.checkpoint_lower_lsn = start + SizeOfXLogLongPHD;
	root.validated_tail_lsn_exclusive = start + XLOG_BLCKSZ + SizeOfXLogShortPHD;
	cluster_shared_config = true; latch = false;
	registry_reads = root_reads = source_reads = 0;
	legacy_pins = exit_callbacks = 0;
	memset(&worker_pool, 0, sizeof(worker_pool));
	for (unsigned i = 0; i < CLUSTER_RECOVERY_WORKER_MAX_SLOTS; i++)
		pg_atomic_init_u32(&worker_pool.slot_state[i], CLUSTER_RECOVERY_WORKER_UNUSED);
	root_result = source_result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* A valid old namespace is a decoy, never a PRE2 source fallback. */
static void
legacy_files(void)
{
	ClusterWalThreadClaim claim;
	char path[MAXPGPATH], name[MAXFNAMELEN], pages[2 * XLOG_BLCKSZ];
	int fd = open(wal_path, O_RDONLY);
	if (fd < 0 || read(fd, pages, sizeof(pages)) != sizeof(pages) || close(fd)) abort();
	cluster_wal_thread_claim_fill(&claim, 2, 1, 42);
	snprintf(path, sizeof(path), "%s/thread_2/pgrac_thread.claim", scratch);
	write_exact(path, &claim, sizeof(claim), sizeof(claim));
	XLogFileName(name, 1, 2, wal_segment_size);
	snprintf(path, sizeof(path), "%s/thread_2/%s", scratch, name);
	write_exact(path, pages, sizeof(pages), wal_segment_size);
}

UT_TEST(test_pre2_uses_selected_generation_without_legacy_latch)
{
	fixture();
	UT_ASSERT_EQ(cluster_recovery_worker_revalidate(2), CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(registry_reads, 0); UT_ASSERT_EQ(root_reads, 1); UT_ASSERT_EQ(source_reads, 1);
}
UT_TEST(test_pre2_claim_and_wal_corruption_refuse)
{
	int fd; uint8 bad = 0;
	fixture();
	fd = open(claim_path, O_WRONLY);
	if (fd < 0 || pwrite(fd, &bad, 1, 0) != 1 || close(fd)) abort();
	UT_ASSERT(cluster_recovery_worker_revalidate(2) != CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(registry_reads, 0);
	fixture(); fd = open(wal_path, O_WRONLY);
	if (fd < 0 || pwrite(fd, &bad, 1, XLOG_BLCKSZ) != 1 || close(fd)) abort();
	UT_ASSERT(cluster_recovery_worker_revalidate(2) != CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(registry_reads, 0);
}
UT_TEST(test_pre2_does_not_fallback_from_missing_generation_or_root)
{
	fixture(); legacy_files(); root.identity.origin_owner_incarnation = 100;
	source.claim.identity = root.identity;
	UT_ASSERT(cluster_recovery_worker_revalidate(2) != CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(registry_reads, 0);
	fixture(); root_result = CLUSTER_CONTROL_ROOT_ABSENT;
	UT_ASSERT_EQ(cluster_recovery_worker_revalidate(2), CLUSTER_RECOVERY_STREAM_UNREADABLE);
	UT_ASSERT_EQ(registry_reads, 0); UT_ASSERT_EQ(source_reads, 0);
}
UT_TEST(test_pre2_source_refusal_is_not_legacy_fallback)
{
	fixture(); source_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	UT_ASSERT(cluster_recovery_worker_revalidate(2) != CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(registry_reads, 0); UT_ASSERT_EQ(source_reads, 1);
}
UT_TEST(test_legacy_registry_selection_is_unchanged)
{
	fixture(); cluster_shared_config = false;
	UT_ASSERT_EQ(cluster_recovery_worker_revalidate(2), CLUSTER_RECOVERY_STREAM_UNREADABLE);
	UT_ASSERT_EQ(registry_reads, 1); UT_ASSERT_EQ(root_reads, 0); UT_ASSERT_EQ(source_reads, 0);
	fixture(); legacy_files(); cluster_shared_config = false; latch = true;
	UT_ASSERT_EQ(cluster_recovery_worker_revalidate(2), CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(registry_reads, 0); UT_ASSERT_EQ(root_reads, 1); UT_ASSERT_EQ(source_reads, 0);
}
UT_TEST(test_launch_pins_complete_pre2_input_before_background_reads)
{
	fixture();
	cluster_recovery_workers_launch();
	UT_ASSERT_EQ(root_reads, 1); UT_ASSERT_EQ(source_reads, 1); UT_ASSERT_EQ(legacy_pins, 0);
	/* A CF read from the worker would now refuse. The original launch cut
	 * remains its only ROOT/source input; physical claim/page reads continue. */
	root_result = source_result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
	CurrentResourceOwner = NULL;
	crypto_owned = crypto_unowned = 0;
	cluster_recovery_worker_main(Int32GetDatum(0));
	UT_ASSERT_EQ(owner_creates, 1);
#if defined(USE_OPENSSL) || defined(PGRAC_TEST_BACKEND_HASH_OWNER)
	UT_ASSERT(crypto_owned > 0);
#endif
	UT_ASSERT_EQ(crypto_unowned, 0);
	UT_ASSERT_EQ(crypto_handles, 0);
	UT_ASSERT_EQ(worker_pool.stream_verdict[2], CLUSTER_RECOVERY_STREAM_OK);
	UT_ASSERT_EQ(pg_atomic_read_u32(&worker_pool.slot_state[0]), CLUSTER_RECOVERY_WORKER_DONE);
	UT_ASSERT_EQ(root_reads, 1); UT_ASSERT_EQ(source_reads, 1); UT_ASSERT_EQ(registry_reads, 0);
	UT_ASSERT_EQ(exit_callbacks, 1);
}
UT_TEST(test_background_keeps_stale_launch_and_alive_refusals)
{
	fixture(); cluster_recovery_workers_launch();
	worker_pool.pre2_input[2].generation--;
	CurrentResourceOwner = NULL;
	cluster_recovery_worker_main(Int32GetDatum(0));
	UT_ASSERT_EQ(worker_pool.stream_verdict[2], CLUSTER_RECOVERY_STREAM_UNREADABLE);
	UT_ASSERT_EQ(root_reads, 1); UT_ASSERT_EQ(source_reads, 1);
	fixture(); root.lifecycle = CLUSTER_CONTROL_ROOT_LIFECYCLE_OPEN;
	root.published_at_usec = GetCurrentTimestamp();
	cluster_recovery_workers_launch();
	CurrentResourceOwner = NULL;
	cluster_recovery_worker_main(Int32GetDatum(0));
	UT_ASSERT_EQ(worker_pool.stream_verdict[2], CLUSTER_RECOVERY_STREAM_SKIPPED);
	UT_ASSERT_EQ(root_reads, 1); UT_ASSERT_EQ(registry_reads, 0);
}
int
main(void)
{
	UT_PLAN(7);
	printf("# volatile worker pool bytes: %zu; selected input bytes: %zu\n",
		   sizeof(ClusterRecoveryWorkerPool), sizeof(ClusterRecoveryWorkerInput));
	UT_RUN(test_pre2_uses_selected_generation_without_legacy_latch);
	UT_RUN(test_pre2_claim_and_wal_corruption_refuse);
	UT_RUN(test_pre2_does_not_fallback_from_missing_generation_or_root);
	UT_RUN(test_pre2_source_refusal_is_not_legacy_fallback);
	UT_RUN(test_legacy_registry_selection_is_unchanged);
	UT_RUN(test_launch_pins_complete_pre2_input_before_background_reads);
	UT_RUN(test_background_keeps_stale_launch_and_alive_refusals);
	if (scratch[0] && !rmtree(scratch, true)) abort();
	UT_DONE(); return ut_failed_count != 0;
}
