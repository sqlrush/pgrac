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

int
main(void)
{
	UT_PLAN(16);
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
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
