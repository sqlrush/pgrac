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
 *	  Tests (U1-U9):
 *	    U1  per-worker isolation: install on shard 0 is invisible to shard 1
 *	    U2  dedup per-shard: MISS -> IN_FLIGHT_DUPLICATE -> CACHED_REPLY
 *	    U3  counter accessors sum across shards
 *	    U4  cross-shard GC: cleanup_on_node_dead reaches every shard
 *	    U5  bounds fail-closed: worker_id out of range -> FULL + misroute++
 *	    U6  N=1 degenerate: only shard 0 valid; worker 1 out of range
 *	    U7  per-shard cap: fill shard 0 to cap -> FULL + full_count++
 *	    U8  cross-shard TTL sweep reaches every shard
 *	    U9  backend-exit cleanup reaches every shard
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
	status->hashp = hashp;
	status->curBucket = 0;
	status->curEntry = NULL;
}

void *
hash_seq_search(HASH_SEQ_STATUS *status)
{
	FakeDedupShardHtab *h = (FakeDedupShardHtab *)status->hashp;

	if (status->curBucket >= (uint32)h->count)
		return NULL;
	return h->entries[status->curBucket++];
}

void
hash_seq_term(HASH_SEQ_STATUS *status pg_attribute_unused())
{}

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
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);

	/* Complete shard 0's entry only. */
	install_granted(0, &key);

	/* Shard 0 now serves a cached reply; shard 1 is untouched (still
	 * in-flight) — proves zero cross-worker sharing. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_CACHED_REPLY);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, &cached),
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

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* retransmit before reply installed */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_IN_FLIGHT_DUPLICATE);
	install_granted(0, &key);
	/* retransmit after reply installed → cached replay */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, &cached),
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

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, &cached),
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

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, &cached);
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
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(99, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_FULL);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(-1, &key, tag, 1, &cached),
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

	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &key, tag, 1, &cached),
				 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	/* worker 1 does not exist when lms_workers=1 → fail-closed. */
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &key, tag, 1, &cached),
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

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &k, tag, 1, &cached),
					 (int)GCS_BLOCK_DEDUP_MISS_REGISTERED);
	}
	{
		GcsBlockDedupKey overflow = make_key(0, 1, 999, 7);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(0, &overflow, tag, 1, &cached),
					 (int)GCS_BLOCK_DEDUP_FULL);
	}
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_full_count(), 1);
	/* shard 1 is unaffected by shard 0 being full. */
	{
		GcsBlockDedupKey k = make_key(0, 2, 500, 7);
		BufferTag t1 = make_tag(61);

		UT_ASSERT_EQ((int)cluster_gcs_block_dedup_lookup_or_register(1, &k, t1, 1, &cached),
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

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, &cached);
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

	(void)cluster_gcs_block_dedup_lookup_or_register(0, &ka, ta, 1, &cached);
	(void)cluster_gcs_block_dedup_lookup_or_register(1, &kb, tb, 1, &cached);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 2);

	cluster_gcs_block_dedup_cleanup_on_backend_exit(0, 9);
	UT_ASSERT_EQ((int)cluster_gcs_block_dedup_get_in_flight_count(), 0);
}


int
main(void)
{
	UT_PLAN(9);
	UT_RUN(u1_per_worker_isolation);
	UT_RUN(u2_dedup_lifecycle_per_shard);
	UT_RUN(u3_counters_sum_across_shards);
	UT_RUN(u4_cleanup_on_node_dead_all_shards);
	UT_RUN(u5_out_of_range_worker_fail_closed);
	UT_RUN(u6_n1_only_shard0);
	UT_RUN(u7_per_shard_cap_full);
	UT_RUN(u8_ttl_sweep_all_shards);
	UT_RUN(u9_backend_exit_cleanup_all_shards);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
