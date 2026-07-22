/*-------------------------------------------------------------------------
 *
 * test_cluster_gcs_block_dedup.c
 *	  Behavioral invariants for the spec-7.3 D5 per-worker sharded GCS
 *	  block dedup HTAB.  spec-2.34 shipped a single global dedup table +
 *	  lock;  spec-7.3 D5 shards it into per-worker private instances
 *	  (dedup_shards[worker_id]) so the LMS worker pool (worker[shard(tag)])
 *	  never contends on one lock and never shares dedup state across
 *	  workers.
 *
 *	  This binary links cluster_gcs_block_dedup.o + cluster_lms_shard.o and
 *	  provides minimal fake-shmem stubs (an N-shard fake HTAB, one array
 *	  per worker) so the real sharded lookup/install/remove/GC/counter
 *	  paths run standalone.  It is the direct proof of "dedup 跨 worker 零
 *	  共享" (spec-7.3 §7 DoD): registering on shard i must be invisible to
 *	  shard j.
 *
 *	  The cross-node behavioral ground truth (2-node retransmit + dedup
 *	  CACHED_REPLY replay at the default cluster.lms_workers=2) lives in
 *	  cluster_tap t/112_gcs_block_retransmit_2node.pl;  the multi-tag ->
 *	  multi-worker dispatch e2e + injection-forced misroute land in D9.
 *
 *	  Tests (U1-U15):
 *	    U1  per-worker isolation: install on shard 0 is invisible to shard 1
 *	    U2  dedup per-shard: MISS -> IN_FLIGHT_DUPLICATE -> CACHED_REPLY
 *	    U3  counter accessors sum across shards
 *	    U4  cross-shard GC: cleanup_on_node_dead reaches every shard
 *	    U5  bounds fail-closed: worker_id out of range -> FULL + misroute++
 *	    U6  N=1 degenerate: only shard 0 valid; worker 1 out of range
 *	    U7  per-shard cap: fill shard 0 to cap -> FULL + full_count++
 *	    U8  cross-shard TTL sweep reaches every shard
 *	    U9  backend-exit cleanup reaches every shard
 *	    U10 remove releases an IN_FLIGHT entry for re-evaluation
 *	    U11 READ_IMAGE forward marker -> FORWARDED; direct serve -> CACHED
 *	    U12 TTL threshold covers the (retries+1) x reply-timeout lifetime
 *	    U13 mark_done truth table: identity gates + idempotent stamp (RC-F)
 *	    U14 TTL posture pinned at registration: hint beats GUCs, no re-read
 *	    U15 DONE linger ages a proven entry out before the full lifetime
 *	    U16 capability-routed registration: violations denied, legacy pinned
 *	        at the protocol maximum (review F5 / calibration 2)
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_gcs_block_dedup.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_lms_shard.h"
#include "cluster/cluster_shmem.h"
#include "port/pg_crc32c.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"

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


UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}


/* ============================================================
 * GUC / global stubs the module reads.
 * ============================================================ */

bool cluster_enabled = true;
int cluster_node_id = 0;
int cluster_lms_workers = 2;
int MaxConnections = 1; /* spec-7.2a D4 floor input (x declared nodes = 1) */
int cluster_gcs_block_dedup_max_entries = 8;
int cluster_gcs_block_retransmit_initial_backoff_ms = 100;
int cluster_gcs_block_retransmit_max_retries = 4;
int cluster_gcs_reply_timeout_ms = 5000;
int MyBackendId = 1;
bool IsUnderPostmaster = true;


/* ============================================================
 * N-shard fake HTAB.  ShmemInitHash hands out one fake shard array per
 * call, in init order (shard 0, 1, ...);  hash_search / hash_seq operate
 * on the shard identified by the returned handle.
 * ============================================================ */

#define FAKE_DEDUP_CAP 8

typedef struct FakeDedupShardHtab {
	char entries[FAKE_DEDUP_CAP][sizeof(GcsBlockDedupEntry)];
	long count;
} FakeDedupShardHtab;

static FakeDedupShardHtab fake_htab[CLUSTER_LMS_MAX_WORKERS];
static int fake_htab_init_seq;
static Size fake_keysize;
static Size fake_entrysize;
static int fake_hash_seq_init_count;
static int fake_hash_seq_term_count;

/* ShmemInitStruct blob for the dedup ctl header + per-shard structs. */
static union {
	uint64 force_align;
	char data[16384];
} fake_dedup_struct;
static bool fake_dedup_struct_found;

static int fake_before_shmem_exit_registered;


static void
reset_fake_dedup(int n_workers, int max_entries)
{
	memset(fake_htab, 0, sizeof(fake_htab));
	fake_htab_init_seq = 0;
	fake_keysize = 0;
	fake_entrysize = 0;
	fake_hash_seq_init_count = 0;
	fake_hash_seq_term_count = 0;
	memset(&fake_dedup_struct, 0, sizeof(fake_dedup_struct));
	fake_dedup_struct_found = false;
	fake_before_shmem_exit_registered = 0;

	cluster_enabled = true;
	cluster_node_id = 0;
	cluster_lms_workers = n_workers;
	cluster_gcs_block_dedup_max_entries = max_entries;

	/* size() then init() — same order the shmem bootstrap uses. */
	(void)cluster_gcs_block_dedup_shmem_size();
	cluster_gcs_block_dedup_shmem_init();
}


/* ============================================================
 * PG-runtime stubs.
 * ============================================================ */

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size, bool *foundPtr)
{
	Assert(size <= sizeof(fake_dedup_struct.data));
	*foundPtr = fake_dedup_struct_found;
	fake_dedup_struct_found = true;
	return fake_dedup_struct.data;
}

HTAB *
ShmemInitHash(const char *name pg_attribute_unused(), long init_size pg_attribute_unused(),
			  long max_size pg_attribute_unused(), HASHCTL *infoP, int hash_flags)
{
	FakeDedupShardHtab *h;

	Assert((hash_flags & HASH_ELEM) != 0);
	Assert(infoP->entrysize == sizeof(GcsBlockDedupEntry));
	Assert(fake_htab_init_seq < CLUSTER_LMS_MAX_WORKERS);
	fake_keysize = infoP->keysize;
	fake_entrysize = infoP->entrysize;
	h = &fake_htab[fake_htab_init_seq++];
	h->count = 0;
	return (HTAB *)h;
}

void *
hash_search(HTAB *hashp, const void *keyPtr, HASHACTION action, bool *foundPtr)
{
	FakeDedupShardHtab *h = (FakeDedupShardHtab *)hashp;
	long i;

	Assert(h != NULL);
	Assert(fake_keysize > 0);

	for (i = 0; i < h->count; i++) {
		char *entry = h->entries[i];

		if (memcmp(entry, keyPtr, fake_keysize) == 0) {
			if (foundPtr != NULL)
				*foundPtr = true;
			if (action == HASH_REMOVE) {
				if (i + 1 < h->count)
					memmove(h->entries[i], h->entries[i + 1],
							(size_t)(h->count - i - 1) * sizeof(GcsBlockDedupEntry));
				h->count--;
				return entry;
			}
			return entry;
		}
	}

	if (foundPtr != NULL)
		*foundPtr = false;
	if (action == HASH_FIND || action == HASH_REMOVE)
		return NULL;
	if (action == HASH_ENTER_NULL && h->count >= FAKE_DEDUP_CAP)
		return NULL;
	if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
		char *entry = h->entries[h->count++];

		memset(entry, 0, sizeof(GcsBlockDedupEntry));
		memcpy(entry, keyPtr, fake_keysize);
		return entry;
	}
	return NULL;
}

void
hash_seq_init(HASH_SEQ_STATUS *status, HTAB *hashp)
{
	fake_hash_seq_init_count++;
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeDedupShardHtab *h = (FakeDedupShardHtab *)status->hashp;

	if (status->curBucket >= (uint32)h->count) {
		hash_seq_term(status);
		return NULL;
	}
	return h->entries[status->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{
	fake_hash_seq_term_count++;
}

Size
hash_estimate_size(long num_entries, Size entrysize)
{
	return (Size)num_entries * entrysize;
}

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

TimestampTz
GetCurrentTimestamp(void)
{
	return (TimestampTz)1000;
}

/* elog(LOG) plumbing for the spec-7.2a D5 saturation LOG-once in the TTL
 * sweep (never fires in these tests: full_count stays below threshold). */
bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false; /* suppress: no message assembly in unit context */
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
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/* spec-7.2a D4: pre-shmem conf sniff — a single declared node keeps the
 * auto-size floor at MaxConnections (=1), so the tests' tiny configured
 * capacities stay in force. */
int
cluster_conf_declared_node_count_early(void)
{
	return 1;
}

Size
add_size(Size s1, Size s2)
{
	return s1 + s2;
}

Size
mul_size(Size s1, Size s2)
{
	return s1 * s2;
}

void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

void
before_shmem_exit(pg_on_exit_callback function pg_attribute_unused(),
				  Datum arg pg_attribute_unused())
{
	fake_before_shmem_exit_registered++;
}


/* ============================================================
 * Test helpers.
 * ============================================================ */

static GcsBlockDedupKey
make_key(uint32 origin, int32 backend, uint64 reqid, uint64 epoch)
{
	GcsBlockDedupKey k;

	memset(&k, 0, sizeof(k));
	k.origin_node_id = origin;
	k.requester_backend_id = backend;
	k.request_id = reqid;
	k.cluster_epoch = epoch;
	return k;
}

static BufferTag
make_tag(uint32 blockno)
{
	BufferTag tag;

	memset(&tag, 0, sizeof(tag));
	tag.spcOid = 1663;
	tag.dbOid = 1;
	tag.relNumber = 100;
	tag.forkNum = MAIN_FORKNUM;
	tag.blockNum = blockno;
	return tag;
}

static void
install_granted(int worker_id, const GcsBlockDedupKey *key)
{
	GcsBlockReplyHeader hdr;
	static char block[GCS_BLOCK_DATA_SIZE];

	memset(&hdr, 0, sizeof(hdr));
	memset(block, 0x5a, sizeof(block));
	cluster_gcs_block_dedup_install_reply(worker_id, key, GCS_BLOCK_REPLY_GRANTED, &hdr, block);
}

static GcsBlockPcmXImageBinding
make_pcm_x_binding(BufferTag tag, uint32 requester_node, uint32 requester_procno,
				   uint64 requester_request_id, uint64 epoch, uint64 image_id,
				   uint64 master_session)
{
	GcsBlockPcmXImageBinding binding;

	memset(&binding, 0, sizeof(binding));
	binding.identity.ref.identity.tag = tag;
	binding.identity.ref.identity.node_id = (int32)requester_node;
	binding.identity.ref.identity.procno = requester_procno;
	binding.identity.ref.identity.xid = (TransactionId)17;
	binding.identity.ref.identity.cluster_epoch = epoch;
	binding.identity.ref.identity.request_id = requester_request_id;
	binding.identity.ref.identity.wait_seq = 19;
	binding.identity.ref.identity.base_own_generation = 23;
	binding.identity.ref.handle.ticket_id = 29;
	binding.identity.ref.handle.queue_generation = 31;
	binding.identity.ref.grant_generation = 37;
	binding.identity.image.image_id = image_id;
	binding.identity.image.source_own_generation = 41;
	binding.identity.image.page_scn = 43;
	binding.identity.image.page_lsn = 47;
	binding.identity.image.source_node = 0;
	binding.identity.image.page_checksum = 53;
	binding.master_session = master_session;
	return binding;
}

static GcsBlockReplyHeader
make_pcm_x_reply_header(const GcsBlockDedupKey *key, const GcsBlockPcmXImageBinding *binding)
{
	GcsBlockReplyHeader hdr;

	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = key->request_id;
	hdr.page_lsn = binding->identity.image.page_lsn;
	hdr.epoch = key->cluster_epoch;
	hdr.checksum = binding->identity.image.page_checksum;
	hdr.sender_node = (int32)binding->identity.image.source_node;
	hdr.requester_backend_id = key->requester_backend_id;
	hdr.transition_id = (uint8)PCM_TRANS_N_TO_S;
	hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	return hdr;
}

static uint32
pcm_x_test_block_checksum(const char *page)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, page, GCS_BLOCK_DATA_SIZE);
	FIN_CRC32C(crc);
	return (uint32)crc;
}

static void
prepare_pcm_x_page(char *page, GcsBlockPcmXImageBinding *binding, GcsBlockReplyHeader *hdr)
{
	PageHeaderData page_header;

	memcpy(&page_header, page, sizeof(page_header));
	PageXLogRecPtrSet(page_header.pd_lsn, (XLogRecPtr)binding->identity.image.page_lsn);
	page_header.pd_block_scn = (SCN)binding->identity.image.page_scn;
	memcpy(page, &page_header, sizeof(page_header));
	binding->identity.image.page_checksum = pcm_x_test_block_checksum(page);
	hdr->page_lsn = binding->identity.image.page_lsn;
	hdr->checksum = binding->identity.image.page_checksum;
}

static GcsBlockPcmXImageResult
stage_pcm_x_ready(int worker_id, const GcsBlockDedupKey *key, const BufferTag *tag,
				  const GcsBlockPcmXImageBinding *binding, const GcsBlockReplyHeader *hdr,
				  const char *page)
{
	GcsBlockPcmXImageBinding reserved = *binding;
	GcsBlockPcmXImageResult result;

	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	result = cluster_gcs_block_dedup_pcm_x_reserve(worker_id, key, tag, &reserved);
	if (result != GCS_BLOCK_PCM_X_IMAGE_RESERVED && result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE)
		return result;
	result = cluster_gcs_block_dedup_pcm_x_materialize(worker_id, key, tag, binding, UINT64_C(41),
													   (uint8)PCM_STATE_X, hdr, page);
	if (result != GCS_BLOCK_PCM_X_IMAGE_STORED && result != GCS_BLOCK_PCM_X_IMAGE_DUPLICATE)
		return result;
	return cluster_gcs_block_dedup_pcm_x_publish_ready_exact(worker_id, key, tag, binding);
}


/* ============================================================
 * U1 — per-worker isolation: an install on shard 0 is invisible to
 * shard 1.  Same key registered on both shards stays independent;
 * completing shard 0's entry does NOT complete shard 1's.
 * ============================================================ */
UT_TEST(u1_per_worker_isolation)
{
	GcsBlockDedupKey key = make_key(0, 1, 42, 7);
	BufferTag tag = make_tag(10);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* Register the same key on shard 0 and shard 1 — separate tables, so
	 * both see a fresh MISS. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	/* Complete shard 0's entry only. */
	install_granted(0, &key);

	/* Shard 0 now serves a cached reply; shard 1 is untouched (still
	 * in-flight) — proves zero cross-worker sharing. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
}


/* ============================================================
 * U2 — dedup lifecycle within one shard.
 * ============================================================ */
UT_TEST(u2_dedup_lifecycle_per_shard)
{
	GcsBlockDedupKey key = make_key(0, 1, 43, 7);
	BufferTag tag = make_tag(11);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* retransmit before reply installed */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	install_granted(0, &key);
	/* retransmit after reply installed → cached replay */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U3 — counter accessors sum across shards.
 * ============================================================ */
UT_TEST(u3_counters_sum_across_shards)
{
	GcsBlockDedupKey ka = make_key(0, 1, 50, 7);
	GcsBlockDedupKey kb = make_key(0, 2, 51, 7);
	BufferTag ta = make_tag(20);
	BufferTag tb = make_tag(21);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	/* miss on shard 0 + miss on shard 1 = 2 (aggregate view). */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_miss_count(), 2);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_full_count(), 0);
}


/* ============================================================
 * U4 — cross-shard GC: node-dead cleanup reaches every shard.
 * ============================================================ */
UT_TEST(u4_cleanup_on_node_dead_all_shards)
{
	GcsBlockDedupKey ka = make_key(3, 1, 60, 7); /* origin node 3 */
	GcsBlockDedupKey kb = make_key(3, 2, 61, 7); /* origin node 3 */
	BufferTag ta = make_tag(30);
	BufferTag tb = make_tag(31);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	cluster_gcs_block_dedup_cleanup_on_node_dead(3);

	/* both shards drained */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U5 — bounds fail-closed: worker_id out of range → FULL + misroute++.
 * ============================================================ */
UT_TEST(u5_out_of_range_worker_fail_closed)
{
	GcsBlockDedupKey key = make_key(0, 1, 70, 7);
	BufferTag tag = make_tag(40);
	GcsBlockDedupEntry cached;
	uint64 before;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	before = cluster_gcs_block_dedup_get_misroute_failclosed_count();

	/* worker_id >= live shard count → fail-closed, no crash, no store. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(99, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_FULL);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(-1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_FULL);

	UT_ASSERT_EQ((int)(cluster_gcs_block_dedup_get_misroute_failclosed_count() - before), 2);
	/* nothing was stored */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U6 — N=1 degenerate: only shard 0 valid; worker 1 out of range.
 * ============================================================ */
UT_TEST(u6_n1_only_shard0)
{
	GcsBlockDedupKey key = make_key(0, 1, 80, 7);
	BufferTag tag = make_tag(50);
	GcsBlockDedupEntry cached;
	uint64 before;

	reset_fake_dedup(1, FAKE_DEDUP_CAP);
	before = cluster_gcs_block_dedup_get_misroute_failclosed_count();

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* worker 1 does not exist when lms_workers=1 → fail-closed. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, 0, false, &cached),
		(int)GCS_BLOCK_DEDUP_FULL);
	UT_ASSERT_EQ((int)(cluster_gcs_block_dedup_get_misroute_failclosed_count() - before), 1);
}


/* ============================================================
 * U7 — per-shard cap: fill shard 0 to cap → FULL + full_count++.
 * ============================================================ */
UT_TEST(u7_per_shard_cap_full)
{
	BufferTag tag = make_tag(60);
	GcsBlockDedupEntry cached;
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	for (i = 0; i < FAKE_DEDUP_CAP; i++) {
		GcsBlockDedupKey k = make_key(0, 1, (uint64)(100 + i), 7);

		UT_ASSERT_EQ(
			(int)cluster_gcs_block_dedup_lookup_or_register(0, &k, tag, 1, 0, false, &cached),
			(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
	{
		GcsBlockDedupKey overflow = make_key(0, 1, 999, 7);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &overflow, tag, 1, 0, false,
																	 &cached),
					 (int)GCS_BLOCK_DEDUP_FULL);
	}
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_full_count(), 1);
	/* shard 1 is unaffected by shard 0 being full. */
	{
		GcsBlockDedupKey k = make_key(0, 2, 500, 7);
		BufferTag t1 = make_tag(61);

		UT_ASSERT_EQ(
			(int)cluster_gcs_block_dedup_lookup_or_register(1, &k, t1, 1, 0, false, &cached),
			(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
}


/* ============================================================
 * U8 — cross-shard TTL sweep reaches every shard.
 * ============================================================ */
UT_TEST(u8_ttl_sweep_all_shards)
{
	GcsBlockDedupKey ka = make_key(0, 1, 200, 7);
	GcsBlockDedupKey kb = make_key(0, 2, 201, 7);
	BufferTag ta = make_tag(70);
	BufferTag tb = make_tag(71);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	/* now far past the expiry threshold → both shards swept. */
	cluster_gcs_block_dedup_sweep_expired((TimestampTz)100000000000LL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U9 — backend-exit cleanup reaches every shard.
 * ============================================================ */
UT_TEST(u9_backend_exit_cleanup_all_shards)
{
	GcsBlockDedupKey ka = make_key(0, 9, 300, 7); /* local origin, backend 9 */
	GcsBlockDedupKey kb = make_key(0, 9, 301, 7); /* local origin, backend 9 */
	BufferTag ta = make_tag(80);
	BufferTag tb = make_tag(81);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, 0, false, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, 0, false, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	cluster_gcs_block_dedup_cleanup_on_backend_exit(0, 9);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U10 — remove releases an IN_FLIGHT entry for re-evaluation.
 *
 *	The retryable-deny paths (DENIED_PENDING_X / direct-land deny) call
 *	cluster_gcs_block_dedup_remove before replying, because the
 *	requester's convergence retry reuses the same key: a leftover
 *	in-flight entry would swallow it as IN_FLIGHT_DUPLICATE until the
 *	TTL sweep (the S3 RC-B reply-timeout burn).  remove must turn the
 *	next same-key lookup back into MISS_REGISTERED.
 * ============================================================ */
UT_TEST(u10_remove_reopens_in_flight_entry)
{
	GcsBlockDedupKey k = make_key(0, 3, 400, 7);
	BufferTag t = make_tag(90);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* leftover in-flight entry swallows the same-key retry ... */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	/* ... and remove re-opens it for a fresh master evaluation. */
	cluster_gcs_block_dedup_remove(0, &k);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}


/* ============================================================
 * U11 — a READ_IMAGE forward MARKER classifies FORWARDED, a master-direct
 * READ_IMAGE cached serve stays CACHED.
 *
 *	The xheld-read FORWARD install stamps forwarding_master_node and
 *	carries no page; treating it as CACHED_REPLY resends a payload-less
 *	header whose never-computed checksum (0) matches the 31-hash of the
 *	all-zero page — a verifying zero-page install at the requester
 *	(PageIsNew false-empty read, 8.A).  The master-DIRECT xheld serve
 *	installs READ_IMAGE with NO_FORWARDING_MASTER + the real page and
 *	must keep resending as a genuine cached reply.
 * ============================================================ */
UT_TEST(u11_read_image_marker_classifies_forwarded)
{
	GcsBlockDedupKey km = make_key(0, 4, 500, 7);
	GcsBlockDedupKey kd = make_key(0, 4, 501, 7);
	BufferTag t = make_tag(95);
	GcsBlockDedupEntry cached;
	GcsBlockReplyHeader hdr;
	static char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* forward MARKER: forwarding_master_node stamped, no payload. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &km, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = 500;
	hdr.sender_node = 2; /* holder id rides here (HC113) */
	hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, 0);
	cluster_gcs_block_dedup_install_reply(0, &km, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &hdr,
										  NULL);
	memset(&cached, 0, sizeof(cached));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &km, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_FORWARDED_DUPLICATE);
	UT_ASSERT_EQ((int)cached.reply_header.sender_node, 2);

	/* master-DIRECT cached serve: NO_FORWARDING_MASTER + real page. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kd, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	memset(&hdr, 0, sizeof(hdr));
	hdr.request_id = 501;
	hdr.sender_node = 0;
	hdr.status = (uint8)GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&hdr, GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	memset(page, 0x3c, sizeof(page));
	cluster_gcs_block_dedup_install_reply(0, &kd, GCS_BLOCK_REPLY_READ_IMAGE_FROM_XHOLDER, &hdr,
										  page);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kd, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U12 — the TTL threshold covers the LEGAL request lifetime.
 *
 *	Every attempt may wait a full cluster.gcs_reply_timeout_ms before its
 *	retry fires, so an in-flight entry is live for up to
 *	(max_retries + 1) x reply_timeout + total backoff.  The pre-fix
 *	threshold (2 x backoff only) swept a still-live request's entry
 *	mid-flight (S3 rig: 25.5s TTL vs 57.75s lifetime) — the late
 *	retransmit then re-registered as MISS and re-executed a request whose
 *	earlier attempt may already have granted.
 * ============================================================ */
UT_TEST(u12_ttl_covers_reply_timeout_lifetime)
{
	GcsBlockDedupKey k = make_key(0, 5, 600, 7);
	BufferTag t = make_tag(96);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	/* S3 rig shape: 8 retries x 50ms backoff base, 5s reply timeout ->
	 * lifetime = 12.75s backoff + 45s reply windows = 57.75s. */
	cluster_gcs_block_retransmit_initial_backoff_ms = 50;
	cluster_gcs_block_retransmit_max_retries = 8;
	cluster_gcs_reply_timeout_ms = 5000;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 57750, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	t0 = GetCurrentTimestamp();

	/* 40s in: inside the legal lifetime — must SURVIVE the sweep. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)40 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	/* far past 2 x lifetime — must be swept. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)300 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	cluster_gcs_block_retransmit_initial_backoff_ms = 100;
	cluster_gcs_block_retransmit_max_retries = 4;
	cluster_gcs_reply_timeout_ms = 5000;
}


/* ============================================================
 * U13 — mark_done truth table (GCS-race round-2 RC-F).
 *
 *	DONE is advisory: every identity or state doubt drops the proof
 *	(done_mismatch_count++) and leaves the TTL backstop in charge.  Only
 *	a full 4-tuple key + tag + transition_id match on a COMPLETED entry
 *	stamps done_at_ts; a duplicate DONE is idempotent-true.
 * ============================================================ */
UT_TEST(u13_mark_done_truth_table)
{
	GcsBlockDedupKey k = make_key(0, 6, 700, 7);
	BufferTag t = make_tag(97);
	BufferTag wrong_tag = make_tag(98);
	GcsBlockDedupEntry cached;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* miss: DONE for a key that was never registered. */
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 1);

	/* in-flight: entry exists but no reply installed (not COMPLETED). */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 2);

	/* completed, but identity mismatches refuse the stamp. */
	install_granted(0, &k);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &wrong_tag, 1));
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &k, &t, 2));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_mismatch_count(), 4);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 0);

	/* exact identity on a COMPLETED entry stamps the proof. */
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 1);

	/* duplicate DONE (retransmit reorder) is idempotent-true. */
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &k, &t, 1));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_done_marked_count(), 2);

	/* the entry still serves its cached reply inside the done-linger
	 * quarantine — DONE never removes it outright. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
}


/* ============================================================
 * U14 — the TTL posture is PINNED at registration (RC-F).
 *
 *	A nonzero wire hint (the requester's own legal lifetime) beats the
 *	master's GUC-derived threshold; and once registered, a master-local
 *	GUC change never re-shortens a live entry's window (GC paths do not
 *	re-read GUCs).
 * ============================================================ */
UT_TEST(u14_pinned_ttl_wire_hint_and_no_guc_reread)
{
	GcsBlockDedupKey kh = make_key(0, 7, 800, 7);
	GcsBlockDedupKey kg = make_key(0, 7, 801, 7);
	BufferTag t = make_tag(99);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* master GUCs describe an ENORMOUS lifetime (~1590s threshold). */
	cluster_gcs_block_retransmit_initial_backoff_ms = 1000;
	cluster_gcs_block_retransmit_max_retries = 8;
	cluster_gcs_reply_timeout_ms = 60000;

	/* hint 1000ms pins 2s: the sweep obeys the requester's budget, not
	 * the master's huge threshold. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kh, t, 1, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	t0 = GetCurrentTimestamp();
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)1 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)3 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* legacy peer (no capability): the PROTOCOL-MAXIMUM lifetime is pinned
	 * at registration (review F5 / calibration 2); shrinking the GUCs
	 * afterwards must NOT shorten the live window. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kg, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	t0 = GetCurrentTimestamp();
	cluster_gcs_block_retransmit_initial_backoff_ms = 50;
	cluster_gcs_block_retransmit_max_retries = 0;
	cluster_gcs_reply_timeout_ms = 100; /* new threshold would be 200ms */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)10 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	cluster_gcs_block_retransmit_initial_backoff_ms = 100;
	cluster_gcs_block_retransmit_max_retries = 4;
	cluster_gcs_reply_timeout_ms = 5000;
}


/* ============================================================
 * U15 — the DONE proof shortens a completed entry to its pinned
 * done-linger quarantine (RC-F).
 *
 *	The wire hint pins lifetime 53s; default GUCs pin linger 10s.  A completed-but-not-done
 *	sibling survives the same sweeps that age out the DONE-proven entry —
 *	the proof, not the timestamps, is what releases the slot early.
 * ============================================================ */
UT_TEST(u15_done_linger_beats_full_lifetime)
{
	GcsBlockDedupKey kd = make_key(0, 8, 900, 7);
	GcsBlockDedupKey ks = make_key(0, 8, 901, 7);
	BufferTag t = make_tag(100);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &kd, t, 1, 26500, true, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &ks, t, 1, 26500, true, &cached),
		(int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	install_granted(0, &kd);
	install_granted(0, &ks);
	UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &kd, &t, 1));
	t0 = GetCurrentTimestamp();

	/* inside the 10s linger both survive. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)5 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	/* past the linger, inside the 53s lifetime: only the DONE-proven
	 * entry ages out. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)11 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(0, &ks, t, 1, 26500, true, &cached),
		(int)GCS_BLOCK_DEDUP_CACHED_REPLY);

	/* past the pinned lifetime the sibling goes too. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)60 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* ============================================================
 * U16 — capability-routed registration (review F5 / calibration 2).
 *
 *	A GCS_DONE_V1-capable peer MUST carry a sane lifetime hint: zero or
 *	over-protocol-maximum is counted and DENIED without claiming a slot.
 *	A legacy peer's window is unknowable, so it pins the protocol-maximum
 *	lifetime (counted) -- capacity pressure surfaces as FULL, never as an
 *	early reclaim.
 * ============================================================ */
UT_TEST(u16_capability_routing_truth_table)
{
	GcsBlockDedupKey kv = make_key(0, 9, 950, 7);
	GcsBlockDedupKey ko = make_key(0, 9, 951, 7);
	GcsBlockDedupKey kl = make_key(0, 9, 952, 7);
	BufferTag t = make_tag(101);
	GcsBlockDedupEntry cached;
	TimestampTz t0;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);

	/* capable + hint 0: protocol violation -> denied, counted, no slot. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kv, t, 1, 0, true, &cached),
				 (int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_hint_violation_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* capable + over-maximum hint (would pin the slot for days): same. */
	UT_ASSERT_EQ(
		(int)cluster_gcs_block_dedup_lookup_or_register(
			0, &ko, t, 1, (uint32)(GCS_BLOCK_DEDUP_MAX_PROTOCOL_LIFETIME_MS + 1), true, &cached),
		(int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_hint_violation_count(), 2);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	/* legacy peer: registered, counted, pinned at the protocol maximum. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &kl, t, 1, 0, false, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_legacy_pin_count(), 1);

	/* far past any GUC posture (600s) but inside the 2x protocol maximum
	 * (3630s): the legacy entry must SURVIVE the sweep... */
	t0 = GetCurrentTimestamp();
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)600 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 1);

	/* ...and past it, age out. */
	cluster_gcs_block_dedup_sweep_expired(t0 + (TimestampTz)3700 * 1000 * 1000);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


/* PCM-X uses the existing 8KB entry payload without moving any established
 * field or increasing the entry size. */
UT_TEST(u17_pcm_x_binding_layout_is_zero_entry_growth)
{
	UT_ASSERT_EQ((int)sizeof(GcsBlockPcmXImageBinding), 144);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, entry_kind), 46);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, pcm_x_master_session), 48);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, reply_header), 56);
	UT_ASSERT_EQ((int)offsetof(GcsBlockDedupEntry, payload_meta), 112);
	UT_ASSERT_EQ((int)sizeof(((GcsBlockDedupEntry *)0)->payload_meta), 128);
	UT_ASSERT_EQ((int)sizeof(GcsBlockDedupEntry), 8472);
}


/* An exact duplicate reuses immutable bytes.  A generic install with the
 * same key must not overwrite a staged PCM-X image. */
UT_TEST(u18_pcm_x_stage_duplicate_and_generic_overwrite_refused)
{
	BufferTag tag = make_tag(110);
	uint64 requester_id = gcs_reqid_requester(1, 2, 77);
	uint64 image_id;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding conflicting_binding;
	GcsBlockReplyHeader hdr;
	GcsBlockReplyHeader conflicting_hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	char overwrite[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 7, &image_id));
	key = make_key(1, 3, image_id, 0);
	binding = make_pcm_x_binding(tag, 1, 5, requester_id, 0, image_id, 101);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x6a, sizeof(page));
	memset(overwrite, 0x7b, sizeof(overwrite));
	prepare_pcm_x_page(page, &binding, &hdr);
	conflicting_binding = binding;
	conflicting_binding.identity.image.page_scn++;
	conflicting_binding.identity.image.page_lsn++;
	conflicting_hdr = make_pcm_x_reply_header(&key, &conflicting_binding);
	prepare_pcm_x_page(overwrite, &conflicting_binding, &conflicting_hdr);

	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(0, &key, &tag, &conflicting_binding,
																UINT64_C(41), (uint8)PCM_STATE_X,
																&conflicting_hdr, overwrite),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);

	/* The generic completion path cannot mutate a dedicated entry. */
	cluster_gcs_block_dedup_install_reply(0, &key, GCS_BLOCK_REPLY_GRANTED, &hdr, overwrite);
	memset(&cached, 0, sizeof(cached));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT(memcmp(cached.block_data, page, sizeof(page)) == 0);
	UT_ASSERT(
		memcmp(&cached.payload_meta.pcm_x_identity, &binding.identity, sizeof(binding.identity))
		== 0);
	UT_ASSERT_EQ((uint64)cached.pcm_x_master_session, (uint64)binding.master_session);
	UT_ASSERT_EQ((int)cached.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_PCM_X_IMAGE);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_replay_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 2);
}


/* Generic lookup, remove and DONE must all reject a dedicated image. */
UT_TEST(u19_pcm_x_entry_isolated_from_generic_lifecycle)
{
	BufferTag tag = make_tag(111);
	uint64 image_id;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 8, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 78), 13, image_id, 102);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x5c, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S, 1,
																 true, &cached),
				 (int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	cluster_gcs_block_dedup_remove(0, &key);
	UT_ASSERT(!cluster_gcs_block_dedup_mark_done(0, &key, &tag, PCM_TRANS_N_TO_S));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 3);
}


/* Wall-clock, backend exit and node death are not application ACKs.  Only
 * the exact terminal binding may retire the image. */
UT_TEST(u20_pcm_x_entry_survives_generic_gc_and_retires_exactly)
{
	BufferTag tag = make_tag(112);
	uint64 image_id;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding wrong;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 9, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 79), 13, image_id, 103);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x4d, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	cluster_gcs_block_dedup_sweep_expired((TimestampTz)INT64_MAX);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 3);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);

	wrong = binding;
	wrong.identity.ref.grant_generation++;
	memset(&cached, 0x5a, sizeof(cached));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &wrong, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cached.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_GENERIC);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &wrong, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_release_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 2);
}


/* A full shared shard refuses staging without reclaiming live generic or
 * dedicated entries. */
UT_TEST(u21_pcm_x_stage_full_is_fail_closed)
{
	BufferTag tag = make_tag(113);
	GcsBlockDedupEntry cached;
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockReplyHeader hdr;
	uint64 image_id;
	char page[GCS_BLOCK_DATA_SIZE];
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	for (i = 0; i < FAKE_DEDUP_CAP; i++) {
		GcsBlockDedupKey ordinary = make_key(0, 1, (uint64)(2000 + i), 13);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
						 0, &ordinary, tag, PCM_TRANS_N_TO_S, 0, false, &cached),
					 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 10, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 80), 13, image_id, 104);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x3e, sizeof(page));
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_FULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_full_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 1);
}


/* Capacity reservation is durable protocol evidence: generic time and
 * process-lifecycle cleanup cannot retire it, and only its exact binding can. */
UT_TEST(u22_pcm_x_reserved_entry_waits_for_exact_release)
{
	BufferTag tag = make_tag(114);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockDedupEntry cached;
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 11, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 81), 13, image_id, 105);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &reserved, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);

	cluster_gcs_block_dedup_sweep_expired((TimestampTz)INT64_MAX);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 3);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &reserved, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &reserved, -1),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_replay_count(), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_release_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 2);
}


/* READY publication validates every byte carrier before changing RESERVED:
 * local source, reply binding, page CRC, page LSN, page SCN and master session. */
UT_TEST(u23_pcm_x_materialize_validation_is_fail_closed_and_byte_stable)
{
	BufferTag tag = make_tag(115);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageBinding bad_binding;
	GcsBlockReplyHeader hdr;
	GcsBlockReplyHeader bad_hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	char bad_page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 12, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 82), 13, image_id, 106);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x2d, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);

	bad_binding = binding;
	bad_binding.identity.image.source_node = 1;
	bad_hdr = hdr;
	bad_hdr.sender_node = 1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_hdr = hdr;
	bad_hdr.sender_node = 1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	memcpy(bad_page, page, sizeof(bad_page));
	bad_page[sizeof(bad_page) - 1] ^= 0x1;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, bad_page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.identity.image.page_lsn++;
	bad_hdr = hdr;
	bad_hdr.page_lsn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.identity.image.page_scn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.identity.image.page_checksum++;
	bad_hdr = hdr;
	bad_hdr.checksum++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &bad_hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_INVALID);

	bad_binding = binding;
	bad_binding.master_session++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &bad_binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 7);
}


/* A canonical image id is intercepted before generic registration, including
 * on a cold miss; there is no legacy fallback entry to complete later. */
UT_TEST(u24_pcm_x_namespace_cannot_register_as_generic)
{
	BufferTag tag = make_tag(116);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockDedupEntry cached;
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 13, &image_id));
	key = make_key(1, 3, image_id, 13);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, PCM_TRANS_N_TO_S, 1,
																 true, &cached),
				 (int)GCS_BLOCK_DEDUP_VALIDATION_FAIL);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);

	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 83), 13, image_id, 107);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_failclosed_count(), 1);
}


/* The LMS image pump must never let a READY resend monopolize a shard while
 * an unmaterialized reservation is waiting.  Once a READY leg is admitted to
 * the outbound ring it disappears from work scans until an exact type-49
 * retransmit positively re-arms it. */
UT_TEST(u25_pcm_x_work_prefers_reserved_and_marks_ready_staged)
{
	BufferTag ready_tag = make_tag(117);
	BufferTag reserved_tag = make_tag(118);
	GcsBlockDedupKey ready_key;
	GcsBlockDedupKey reserved_key;
	GcsBlockPcmXImageBinding ready_binding;
	GcsBlockPcmXImageBinding reserved_binding;
	GcsBlockPcmXImageBinding wrong_floor;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	uint64 ready_image_id;
	uint64 reserved_image_id;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 14, &ready_image_id));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 15, &reserved_image_id));
	ready_key = make_key(1, 3, ready_image_id, 13);
	reserved_key = make_key(1, 4, reserved_image_id, 13);
	ready_binding = make_pcm_x_binding(ready_tag, 1, 5, gcs_reqid_requester(1, 2, 84), 13,
									   ready_image_id, 108);
	hdr = make_pcm_x_reply_header(&ready_key, &ready_binding);
	memset(page, 0x1d, sizeof(page));
	prepare_pcm_x_page(page, &ready_binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &ready_key, &ready_tag, &ready_binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	reserved_binding = make_pcm_x_binding(reserved_tag, 1, 6, gcs_reqid_requester(1, 3, 85), 13,
										  reserved_image_id, 109);
	reserved_binding.identity.image.page_scn = 0;
	reserved_binding.identity.image.page_lsn = 0;
	reserved_binding.identity.image.page_checksum = 0;
	reserved_binding.required_page_scn = UINT64_C(72057594037950810);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &reserved_key, &reserved_tag,
														&reserved_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	wrong_floor = reserved_binding;
	wrong_floor.required_page_scn++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &reserved_key, &reserved_tag,
														&wrong_floor),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);

	memset(&work, 0, sizeof(work));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(memcmp(&work.key, &reserved_key, sizeof(reserved_key)), 0);
	UT_ASSERT_EQ(memcmp(&work.binding, &reserved_binding, sizeof(reserved_binding)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &reserved_key, &reserved_tag,
																  &reserved_binding, -1),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);

	memset(&work, 0, sizeof(work));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.key, &ready_key, sizeof(ready_key)), 0);
	UT_ASSERT_EQ(memcmp(&work.binding, &ready_binding, sizeof(ready_binding)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &ready_key, &ready_tag,
																	  &ready_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STAGED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &ready_key, &ready_tag,
																	  &ready_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
}


/* A retransmitted type 49 is the only positive evidence that an admitted
 * type 50 needs replay.  Rearm accepts the reservation identity (page fields
 * still zero) but validates the complete ticket/generation/session tuple;
 * an almost-equal ticket cannot make a READY image sendable again. */
UT_TEST(u26_pcm_x_ready_rearm_is_exact)
{
	BufferTag tag = make_tag(119);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageBinding wrong;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	uint64 image_id;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 16, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 86), 13, image_id, 110);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x0d, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STAGED);

	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	wrong = reserved;
	wrong.identity.ref.grant_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_rearm_exact(0, &key, &tag, &wrong),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_rearm_exact(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REARMED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.binding, &binding, sizeof(binding)), 0);
}


/* One pinned holder must not make the hash table's first RESERVED entry
 * monopolize every LMS tick.  Work selection is process-local round robin;
 * the second scan must advance to the other exact reservation. */
UT_TEST(u27_pcm_x_reserved_work_scan_rotates)
{
	BufferTag tag_a = make_tag(120);
	BufferTag tag_b = make_tag(121);
	GcsBlockDedupKey key_a;
	GcsBlockDedupKey key_b;
	GcsBlockPcmXImageBinding binding_a;
	GcsBlockPcmXImageBinding binding_b;
	GcsBlockPcmXImageWork first;
	GcsBlockPcmXImageWork second;
	uint64 image_a;
	uint64 image_b;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 17, &image_a));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 18, &image_b));
	key_a = make_key(1, 3, image_a, 13);
	key_b = make_key(1, 4, image_b, 13);
	binding_a = make_pcm_x_binding(tag_a, 1, 5, gcs_reqid_requester(1, 2, 87), 13, image_a, 111);
	binding_b = make_pcm_x_binding(tag_b, 1, 6, gcs_reqid_requester(1, 3, 88), 13, image_b, 112);
	binding_a.identity.image.page_scn = binding_a.identity.image.page_lsn = 0;
	binding_a.identity.image.page_checksum = 0;
	binding_b.identity.image.page_scn = binding_b.identity.image.page_lsn = 0;
	binding_b.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_a, &tag_a, &binding_a),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_b, &tag_b, &binding_b),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &first),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &second),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT(memcmp(&first.key, &second.key, sizeof(first.key)) != 0);
}


/* The LMS tick must not rescan a potentially multi-thousand-entry generic
 * dedup shard forever when no PCM-X byte work exists.  One initial scan may
 * establish the empty hint; an exact reservation must wake it again.  The
 * fake scan also mirrors dynahash's natural auto-term, so one full scan has
 * exactly one registration and one termination. */
UT_TEST(u28_pcm_x_idle_hint_avoids_empty_rescan_and_reserve_rearms)
{
	BufferTag tag = make_tag(122);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageWork work;
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 1);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 1);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 1);

	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 19, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 89), 13, image_id, 113);
	binding.identity.image.page_scn = binding.identity.image.page_lsn = 0;
	binding.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(fake_hash_seq_init_count, 2);
	UT_ASSERT_EQ(fake_hash_seq_term_count, 2);
}


/* Materialized bytes are retained evidence, not a sendable READY image.  The
 * ownership X->N commit must happen between materialize and the explicit
 * publication call.  A live owner must receive a distinct commit-only work
 * token so conditional BufferContent contention can retry without recopying,
 * aborting the A-record, or synthesizing type 50. */
UT_TEST(u29_pcm_x_materialized_bytes_require_explicit_ready_publication)
{
	BufferTag tag = make_tag(123);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 20, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 90), 13, image_id, 114);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x4e, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ((int)work.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_PCM_X_MATERIALIZED_UNCOMMITTED);
	UT_ASSERT_EQ(memcmp(&work.binding, &binding, sizeof(binding)), 0);
	UT_ASSERT_EQ(work.reservation_token, UINT64_C(41));
	UT_ASSERT_EQ((int)work.source_pcm_state, (int)PCM_STATE_X);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_publish_ready_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_stage_count(), 1);
}


/* A replacement LMS must never infer progress from retained holder evidence.
 * Startup audit is read-only: it detects any dedicated entry and leaves the
 * exact bytes/reservation available for the recovery layer. */
UT_TEST(u30_pcm_x_owner_restart_audit_detects_and_retains_evidence)
{
	BufferTag tag = make_tag(124);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(!cluster_gcs_block_dedup_pcm_x_restart_audit(0));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 21, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 91), 13, image_id, 115);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x5f, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(41), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_restart_audit(0));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
}


/* A pinned reservation must not starve an already READY image forever.  The
 * two work classes share one LMS tick budget, so when both remain runnable a
 * READY leg must be selected no later than the tick after RESERVED. */
UT_TEST(u31_pcm_x_work_classes_alternate_when_both_remain_runnable)
{
	BufferTag ready_tag = make_tag(125);
	BufferTag reserved_tag = make_tag(126);
	GcsBlockDedupKey ready_key;
	GcsBlockDedupKey reserved_key;
	GcsBlockPcmXImageBinding ready_binding;
	GcsBlockPcmXImageBinding reserved_binding;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 ready_image_id;
	uint64 reserved_image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 22, &ready_image_id));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 23, &reserved_image_id));
	ready_key = make_key(1, 3, ready_image_id, 13);
	reserved_key = make_key(1, 4, reserved_image_id, 13);
	ready_binding = make_pcm_x_binding(ready_tag, 1, 5, gcs_reqid_requester(1, 2, 92), 13,
									   ready_image_id, 116);
	hdr = make_pcm_x_reply_header(&ready_key, &ready_binding);
	memset(page, 0x60, sizeof(page));
	prepare_pcm_x_page(page, &ready_binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &ready_key, &ready_tag, &ready_binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	reserved_binding = make_pcm_x_binding(reserved_tag, 1, 6, gcs_reqid_requester(1, 3, 93), 13,
										  reserved_image_id, 117);
	reserved_binding.identity.image.page_scn = 0;
	reserved_binding.identity.image.page_lsn = 0;
	reserved_binding.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &reserved_key, &reserved_tag,
															&reserved_binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(memcmp(&work.key, &reserved_key, sizeof(reserved_key)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.key, &ready_key, sizeof(ready_key)), 0);
}


/* Outbound admission is a small transaction: the exact READY entry is marked
 * first, and a ring refusal must roll that marker back so the image remains
 * runnable.  Repeating the rollback is an idempotent no-op, never corruption. */
UT_TEST(u32_pcm_x_staged_marker_rolls_back_exactly)
{
	BufferTag tag = make_tag(127);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding wrong;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 24, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 94), 13, image_id, 118);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x61, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_mark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STAGED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);

	wrong = binding;
	wrong.identity.ref.grant_generation++;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(0, &key, &tag, &wrong),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REARMED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_REPLAY);
	UT_ASSERT_EQ(memcmp(&work.key, &key, sizeof(key)), 0);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_unmark_staged_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
}


/* Shape-B: a legacy S request may already be registered or forwarded before
 * the PCM-X queue publishes its pending-X claim.  The queue arm must revoke
 * every still-live grant/forward right under the dedup lock, cache an exact
 * retry denial for loss/replay, and fence a late producer from restoring a
 * GRANTED reply.  DONE stops periodic replay; a new request identity remains
 * a normal MISS after the X round. */
UT_TEST(u33_pending_x_arm_terminates_inflight_legacy_s_exactly)
{
	BufferTag tag = make_tag(130);
	BufferTag other_tag = make_tag(131);
	GcsBlockDedupKey inflight = make_key(1, 3, 3001, 17);
	GcsBlockDedupKey forwarded = make_key(2, 4, 3002, 17);
	GcsBlockDedupKey unrelated = make_key(3, 5, 3003, 17);
	GcsBlockDedupKey writer = make_key(1, 6, 3004, 17);
	GcsBlockDedupKey fresh = make_key(1, 3, 4001, 17);
	GcsBlockReplyHeader forward_hdr;
	GcsBlockReplyHeader late_granted;
	GcsBlockDedupEntry cached;
	GcsBlockDedupEntry denied;
	GcsBlockDedupKey denied_keys[2];
	char page[GCS_BLOCK_DATA_SIZE];
	int result;
	int i;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&forward_hdr, 0, sizeof(forward_hdr));
	memset(&late_granted, 0, sizeof(late_granted));
	memset(page, 0x6d, sizeof(page));

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &inflight, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &forwarded, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	forward_hdr.request_id = forwarded.request_id;
	forward_hdr.epoch = forwarded.cluster_epoch;
	forward_hdr.sender_node = 3;
	forward_hdr.requester_backend_id = forwarded.requester_backend_id;
	forward_hdr.transition_id = (uint8)PCM_TRANS_N_TO_S;
	forward_hdr.status = (uint8)GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER;
	GcsBlockReplyHeaderSetForwardingMasterNode(&forward_hdr, cluster_node_id);
	cluster_gcs_block_dedup_install_reply(0, &forwarded,
										 GCS_BLOCK_REPLY_GRANTED_FROM_HOLDER, &forward_hdr, NULL);

	/* Different tag and same-tag writer identities are not legacy-S victims. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &unrelated, other_tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &writer, tag, PCM_TRANS_N_TO_X, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	for (i = 0; i < 2; i++) {
		memset(&denied, 0, sizeof(denied));
		result = cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied);
		UT_ASSERT_EQ(result, GCS_BLOCK_PENDING_X_DENY_NEW);
		UT_ASSERT_EQ((int)denied.entry_kind, (int)GCS_BLOCK_DEDUP_ENTRY_GENERIC);
		UT_ASSERT_EQ((int)denied.transition_id, (int)PCM_TRANS_N_TO_S);
		UT_ASSERT_EQ((int)denied.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
		UT_ASSERT_EQ((int)denied.reply_header.status,
					 (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
		UT_ASSERT_EQ(denied.reply_header.request_id, denied.key.request_id);
		UT_ASSERT_EQ(denied.reply_header.epoch, denied.key.cluster_epoch);
		UT_ASSERT_EQ(denied.reply_header.requester_backend_id,
					 denied.key.requester_backend_id);
		UT_ASSERT_EQ((int)denied.reply_header.transition_id, (int)PCM_TRANS_N_TO_S);
		UT_ASSERT_EQ(denied.reply_header.sender_node, cluster_node_id);
		UT_ASSERT_EQ(GcsBlockReplyHeaderGetForwardingMasterNode(&denied.reply_header),
					 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
		denied_keys[i] = denied.key;
	}
	UT_ASSERT(memcmp(&denied_keys[0], &denied_keys[1], sizeof(denied_keys[0])) != 0);

	/* Initial denial loss is recovered by periodic replay of the same exact
	 * identity, without minting another request or reviving FORWARD. */
	memset(&denied, 0, sizeof(denied));
	UT_ASSERT_EQ(cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied),
				 GCS_BLOCK_PENDING_X_DENY_REPLAY);
	UT_ASSERT(memcmp(&denied.key, &denied_keys[0], sizeof(denied.key)) == 0
			  || memcmp(&denied.key, &denied_keys[1], sizeof(denied.key)) == 0);

	/* An asynchronous old GRANTED producer has lost its right once the deny
	 * is cached; installing it must be a zero-mutation no-op. */
	late_granted.request_id = denied.key.request_id;
	late_granted.epoch = denied.key.cluster_epoch;
	late_granted.sender_node = cluster_node_id;
	late_granted.requester_backend_id = denied.key.requester_backend_id;
	late_granted.transition_id = (uint8)PCM_TRANS_N_TO_S;
	late_granted.status = (uint8)GCS_BLOCK_REPLY_GRANTED;
	cluster_gcs_block_dedup_install_reply(0, &denied.key, GCS_BLOCK_REPLY_GRANTED,
										 &late_granted, page);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &denied.key, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cached.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)cached.reply_header.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);

	/* Duplicate DONE is idempotent and removes both old identities from the
	 * denial replay set; unrelated entries remain in their original states. */
	for (i = 0; i < 2; i++) {
		UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &denied_keys[i], &tag,
											 PCM_TRANS_N_TO_S));
		UT_ASSERT(cluster_gcs_block_dedup_mark_done(0, &denied_keys[i], &tag,
											 PCM_TRANS_N_TO_S));
	}
	UT_ASSERT_EQ(cluster_gcs_block_dedup_pending_x_deny_next(0, &tag, &denied),
				 GCS_BLOCK_PENDING_X_DENY_NOT_FOUND);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &unrelated, other_tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &writer, tag, PCM_TRANS_N_TO_X, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);

	/* Once the queue round is over, a reader with a fresh identity is admitted
	 * normally; the two denied identities cannot poison the new request. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &fresh, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
}

/* A reader arriving after the queue claim is registered only to make its
 * denial reliable: exact arbitration replaces even a pre-existing cached
 * grant before the handler can take a normal dedup shortcut.  The original
 * direct-land property remains attached to the cached denial for replay. */
UT_TEST(u34_pending_x_new_reader_exact_deny_precedes_cached_shortcut)
{
	BufferTag tag = make_tag(132);
	GcsBlockDedupKey key = make_key(2, 7, 5001, 19);
	GcsBlockDedupKey absent = make_key(2, 7, 5002, 19);
	GcsBlockReplyHeader granted;
	GcsBlockDedupEntry cached;
	GcsBlockDedupEntry denied;
	char page[GCS_BLOCK_DATA_SIZE];

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	memset(&granted, 0, sizeof(granted));
	memset(page, 0x71, sizeof(page));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT(cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_S, GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));

	granted.request_id = key.request_id;
	granted.epoch = key.cluster_epoch;
	granted.sender_node = cluster_node_id;
	granted.requester_backend_id = key.requester_backend_id;
	granted.transition_id = (uint8)PCM_TRANS_N_TO_S;
	granted.status = (uint8)GCS_BLOCK_REPLY_GRANTED;
	GcsBlockReplyHeaderSetForwardingMasterNode(&granted,
										 GCS_BLOCK_REPLY_NO_FORWARDING_MASTER);
	cluster_gcs_block_dedup_install_reply(0, &key, GCS_BLOCK_REPLY_GRANTED, &granted, page);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_NEW);
	UT_ASSERT_EQ((int)denied.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);
	UT_ASSERT_EQ((int)denied.request_flags,
				 (int)(GCS_BLOCK_DEDUP_REQUEST_F_PINNED
					   | GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
					 0, &key, &tag, PCM_TRANS_N_TO_S, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_REPLAY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cached.status, (int)GCS_BLOCK_REPLY_DENIED_PENDING_X);

	/* Missing and mismatched identities are zero-mutation invalid results. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pending_x_deny_exact(
					 0, &absent, &tag, PCM_TRANS_N_TO_S, &denied),
				 (int)GCS_BLOCK_PENDING_X_DENY_INVALID);
	UT_ASSERT(!cluster_gcs_block_dedup_set_request_flags_exact(
		0, &key, &tag, PCM_TRANS_N_TO_X, GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(
					 0, &key, tag, PCM_TRANS_N_TO_S, 1000, true, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cached.request_flags,
				 (int)(GCS_BLOCK_DEDUP_REQUEST_F_PINNED
					   | GCS_BLOCK_DEDUP_REQUEST_F_DIRECT_LAND));
}


/* A contended tag A may remain at the commit-only boundary, but its exact
 * retry must not prevent the same DATA worker from advancing independent tag
 * B.  This exercises the production HTAB scan/cursor, not a scheduler model. */
UT_TEST(u35_pcm_x_commit_pending_rotates_to_independent_reserved_tag)
{
	BufferTag tag_a = make_tag(130);
	BufferTag tag_b = make_tag(131);
	GcsBlockDedupKey key_a;
	GcsBlockDedupKey key_b;
	GcsBlockPcmXImageBinding binding_a;
	GcsBlockPcmXImageBinding reserved_a;
	GcsBlockPcmXImageBinding reserved_b;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr_a;
	char page_a[GCS_BLOCK_DATA_SIZE];
	uint64 image_a;
	uint64 image_b;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 30, &image_a));
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 31, &image_b));
	key_a = make_key(1, 3, image_a, 13);
	key_b = make_key(1, 4, image_b, 13);
	binding_a = make_pcm_x_binding(tag_a, 1, 5, gcs_reqid_requester(1, 2, 100), 13, image_a, 120);
	hdr_a = make_pcm_x_reply_header(&key_a, &binding_a);
	memset(page_a, 0x68, sizeof(page_a));
	prepare_pcm_x_page(page_a, &binding_a, &hdr_a);
	reserved_a = binding_a;
	reserved_a.identity.image.page_scn = 0;
	reserved_a.identity.image.page_lsn = 0;
	reserved_a.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_a, &tag_a, &reserved_a),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(0, &key_a, &tag_a, &binding_a,
																UINT64_C(51), (uint8)PCM_STATE_X,
																&hdr_a, page_a),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	reserved_b = make_pcm_x_binding(tag_b, 1, 6, gcs_reqid_requester(1, 3, 101), 13, image_b, 121);
	reserved_b.identity.image.page_scn = 0;
	reserved_b.identity.image.page_lsn = 0;
	reserved_b.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key_b, &tag_b, &reserved_b),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ(memcmp(&work.key, &key_a, sizeof(key_a)), 0);
	UT_ASSERT_EQ(work.reservation_token, UINT64_C(51));
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ(memcmp(&work.key, &key_b, sizeof(key_b)), 0);
}


/* A local terminal DRAIN is not an ACK boundary by itself.  Exact byte and
 * descriptor cleanup publishes a replayable tombstone; duplicate DRAIN stays
 * provably complete until the matching RETIRE watermark removes that proof. */
UT_TEST(u36_pcm_x_drain_cleanup_is_replayable_until_exact_retire)
{
	BufferTag tag = make_tag(132);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockReplyHeader hdr;
	GcsBlockDedupEntry cached;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 32, &image_id));
	/* The initial live cluster episode is epoch zero; RETIRE must preserve the
	 * same exact-match semantics there as after the first reconfiguration. */
	key = make_key(1, 3, image_id, 0);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 102), 0, image_id, 122);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x79, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	UT_ASSERT_EQ((int)stage_pcm_x_ready(0, &key, &tag, &binding, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_drain_status_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_drain_status_exact(0, &key, &tag, &binding),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, 2),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_lookup(0, &key, &tag, &binding, &cached),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_FOUND);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_DUPLICATE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_rearm_exact(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_NOT_READY);

	cluster_gcs_block_dedup_sweep_expired((TimestampTz)INT64_MAX);
	cluster_gcs_block_dedup_cleanup_on_backend_exit(1, 3);
	cluster_gcs_block_dedup_cleanup_on_node_dead(1);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to(12, 2, 122, 29));
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to(0, 1, 122, 29));
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to(0, 2, 121, 29));
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to(0, 2, 122, 28));
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT(cluster_gcs_block_dedup_pcm_x_retire_up_to(0, 2, 122, 29));
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_pcm_x_release_count(), 1);
}


/* A FlushBuffer ERROR occurs after materialization.  Its catch handler must
 * validate, but never delete or rewrite, the immutable A-record and revoke
 * token needed by recovery. */
UT_TEST(u37_pcm_x_finish_error_preserves_exact_materialized_evidence)
{
	BufferTag tag = make_tag(133);
	GcsBlockDedupKey key;
	GcsBlockPcmXImageBinding binding;
	GcsBlockPcmXImageBinding reserved;
	GcsBlockPcmXImageWork work;
	GcsBlockReplyHeader hdr;
	char page[GCS_BLOCK_DATA_SIZE];
	uint64 image_id;

	reset_fake_dedup(2, FAKE_DEDUP_CAP);
	UT_ASSERT(cluster_pcm_x_image_id_encode(2, 33, &image_id));
	key = make_key(1, 3, image_id, 13);
	binding = make_pcm_x_binding(tag, 1, 5, gcs_reqid_requester(1, 2, 103), 13, image_id, 123);
	hdr = make_pcm_x_reply_header(&key, &binding);
	memset(page, 0x6a, sizeof(page));
	prepare_pcm_x_page(page, &binding, &hdr);
	reserved = binding;
	reserved.identity.image.page_scn = 0;
	reserved.identity.image.page_lsn = 0;
	reserved.identity.image.page_checksum = 0;
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_reserve(0, &key, &tag, &reserved),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RESERVED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_materialize(
					 0, &key, &tag, &binding, UINT64_C(61), (uint8)PCM_STATE_X, &hdr, page),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STORED);

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(
					 0, &key, &tag, &binding, UINT64_C(62), (uint8)PCM_STATE_X),
				 (int)GCS_BLOCK_PCM_X_IMAGE_STALE);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_preserve_finish_error_exact(
					 0, &key, &tag, &binding, UINT64_C(61), (uint8)PCM_STATE_X),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 1);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_next_work(0, &work),
				 (int)GCS_BLOCK_PCM_X_IMAGE_COMMIT_PENDING);
	UT_ASSERT_EQ(memcmp(&work.binding, &binding, sizeof(binding)), 0);
	UT_ASSERT_EQ(work.reservation_token, UINT64_C(61));
	UT_ASSERT_EQ((int)work.source_pcm_state, (int)PCM_STATE_X);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_pcm_x_release_exact(0, &key, &tag, &binding, -1),
				 (int)GCS_BLOCK_PCM_X_IMAGE_RELEASED);
	UT_ASSERT_EQ((uint64)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}

int
main(void)
{
	UT_PLAN(37);
	UT_RUN(u1_per_worker_isolation);
	UT_RUN(u2_dedup_lifecycle_per_shard);
	UT_RUN(u3_counters_sum_across_shards);
	UT_RUN(u4_cleanup_on_node_dead_all_shards);
	UT_RUN(u5_out_of_range_worker_fail_closed);
	UT_RUN(u6_n1_only_shard0);
	UT_RUN(u7_per_shard_cap_full);
	UT_RUN(u8_ttl_sweep_all_shards);
	UT_RUN(u9_backend_exit_cleanup_all_shards);
	UT_RUN(u10_remove_reopens_in_flight_entry);
	UT_RUN(u11_read_image_marker_classifies_forwarded);
	UT_RUN(u12_ttl_covers_reply_timeout_lifetime);
	UT_RUN(u13_mark_done_truth_table);
	UT_RUN(u14_pinned_ttl_wire_hint_and_no_guc_reread);
	UT_RUN(u15_done_linger_beats_full_lifetime);
	UT_RUN(u16_capability_routing_truth_table);
	UT_RUN(u17_pcm_x_binding_layout_is_zero_entry_growth);
	UT_RUN(u18_pcm_x_stage_duplicate_and_generic_overwrite_refused);
	UT_RUN(u19_pcm_x_entry_isolated_from_generic_lifecycle);
	UT_RUN(u20_pcm_x_entry_survives_generic_gc_and_retires_exactly);
	UT_RUN(u21_pcm_x_stage_full_is_fail_closed);
	UT_RUN(u22_pcm_x_reserved_entry_waits_for_exact_release);
	UT_RUN(u23_pcm_x_materialize_validation_is_fail_closed_and_byte_stable);
	UT_RUN(u24_pcm_x_namespace_cannot_register_as_generic);
	UT_RUN(u25_pcm_x_work_prefers_reserved_and_marks_ready_staged);
	UT_RUN(u26_pcm_x_ready_rearm_is_exact);
	UT_RUN(u27_pcm_x_reserved_work_scan_rotates);
	UT_RUN(u28_pcm_x_idle_hint_avoids_empty_rescan_and_reserve_rearms);
	UT_RUN(u29_pcm_x_materialized_bytes_require_explicit_ready_publication);
	UT_RUN(u30_pcm_x_owner_restart_audit_detects_and_retains_evidence);
	UT_RUN(u31_pcm_x_work_classes_alternate_when_both_remain_runnable);
	UT_RUN(u32_pcm_x_staged_marker_rolls_back_exactly);
	UT_RUN(u33_pending_x_arm_terminates_inflight_legacy_s_exactly);
	UT_RUN(u34_pending_x_new_reader_exact_deny_precedes_cached_shortcut);
	UT_RUN(u35_pcm_x_commit_pending_rotates_to_independent_reserved_tag);
	UT_RUN(u36_pcm_x_drain_cleanup_is_replayable_until_exact_retire);
	UT_RUN(u37_pcm_x_finish_error_preserves_exact_materialized_evidence);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
