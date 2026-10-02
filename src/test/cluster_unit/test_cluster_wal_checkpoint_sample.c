/* Actual checkpoint observation cache; allocation and short spinlocks are
 * local fixtures. No ROOT/WAL/GC authority is supplied by these readings.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "access/xlog.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "cluster/cluster_wal_thread.h"
#include "cluster/cluster_wal_state.h"
#include "unit_test.h"

UT_DEFINE_GLOBALS();
bool cluster_enabled = true, cluster_shared_config = true;
int cluster_node_id = 1;
BackendType MyBackendType = B_CHECKPOINTER;
static struct ClusterWalThreadShmemData *cluster_wal_thread_shmem;
static bool locked;
uint16
cluster_wal_thread_id(void)
{
	return 2;
}
void
ExceptionalCondition(const char *c, const char *f, int line)
{
	abort();
}
#undef SpinLockAcquire
#undef SpinLockRelease
#define SpinLockAcquire(lock)                                                                      \
	do {                                                                                           \
		UT_ASSERT(!locked);                                                                        \
		locked = true;                                                                             \
	} while (0)
#define SpinLockRelease(lock)                                                                      \
	do {                                                                                           \
		UT_ASSERT(locked);                                                                         \
		locked = false;                                                                            \
	} while (0)
#include "test_cluster_wal_checkpoint_sample.inc"
static ClusterWalThreadShmemData shared;

static ClusterControlRootSnapshot
reset_sample(void)
{
	ClusterControlRootSnapshot record = { 0 };
	memset(&shared, 0, sizeof(shared));
	cluster_wal_thread_shmem = &shared;
	cluster_enabled = cluster_shared_config = true;
	MyBackendType = B_CHECKPOINTER;
	locked = false;
	shared.dir_validated = true;
	pg_atomic_init_u32(&shared.writer_ref_state, 2);
	shared.v2_ref.claim.identity.system_identifier = 123;
	shared.v2_ref.claim.identity.origin_node_id = cluster_node_id;
	shared.v2_ref.claim.identity.origin_thread_id = 2;
	shared.v2_ref.claim.identity.origin_owner_incarnation = 40;
	shared.v2_ref.claim.identity.root_lineage_seq = 8;
	shared.v2_ref.timeline = 1;
	record.identity = shared.v2_ref.claim.identity;
	record.root_publish_seq = 5;
	record.checkpoint_tli = record.tail_tli = 1;
	record.checkpoint_lower_lsn = 100;
	record.tail_last_record_lsn = 400;
	record.validated_tail_lsn_exclusive = 500;
	record.published_at_usec = 1000;
	return record;
}

UT_TEST(sample_is_unknown_until_exact_current_writer_checkpoint)
{
	ClusterControlRootSnapshot record = reset_sample();
	ClusterWalThreadCheckpointSampleV1 sample, zero = { 0 };
	memset(&sample, 0xa5, sizeof(sample));
	UT_ASSERT(!cluster_wal_thread_checkpoint_sample_v1(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	cluster_wal_thread_checkpoint_observed_v1(&record, 300);
	UT_ASSERT(cluster_wal_thread_checkpoint_sample_v1(&sample));
	UT_ASSERT_EQ(sample.root_publish_seq, 5);
	UT_ASSERT_EQ(sample.retained_lower, 100);
	UT_ASSERT_EQ(sample.native_redo, 300);
	UT_ASSERT_EQ(sample.validated_tail, 500);
	UT_ASSERT_EQ(sample.published_at_usec, 1000);
	UT_ASSERT(!locked);
}

UT_TEST(invalid_or_old_checkpoint_never_replaces_last_observation)
{
	for (unsigned fault = 0; fault < 10; fault++) {
		ClusterControlRootSnapshot record = reset_sample();
		ClusterWalThreadCheckpointSampleV1 before, after;
		XLogRecPtr redo = 350;
		cluster_wal_thread_checkpoint_observed_v1(&record, 300);
		UT_ASSERT(cluster_wal_thread_checkpoint_sample_v1(&before));
		record.root_publish_seq++;
		if (fault == 0)
			record.identity.origin_owner_incarnation++;
		if (fault == 1)
			record.identity.origin_node_id++;
		if (fault == 2)
			record.checkpoint_tli++;
		if (fault == 3)
			record.tail_tli++;
		if (fault == 4)
			record.checkpoint_lower_lsn = 0;
		if (fault == 5)
			redo = 99;
		if (fault == 6)
			redo = 401;
		if (fault == 7)
			record.validated_tail_lsn_exclusive = 400;
		if (fault == 8)
			record.root_publish_seq = 4;
		if (fault == 9)
			MyBackendType = B_BACKEND;
		cluster_wal_thread_checkpoint_observed_v1(&record, redo);
		UT_ASSERT(cluster_wal_thread_checkpoint_sample_v1(&after));
		UT_ASSERT_EQ(memcmp(&before, &after, sizeof(before)), 0);
		UT_ASSERT(!locked);
	}
}

UT_TEST(new_checkpoint_replaces_whole_sample_and_unbound_reader_gets_unknown)
{
	ClusterControlRootSnapshot record = reset_sample();
	ClusterWalThreadCheckpointSampleV1 sample, zero = { 0 };
	cluster_wal_thread_checkpoint_observed_v1(&record, 300);
	++record.root_publish_seq;
	record.tail_last_record_lsn = 900;
	record.validated_tail_lsn_exclusive = 1000;
	record.published_at_usec = 2000;
	cluster_wal_thread_checkpoint_observed_v1(&record, 800);
	UT_ASSERT(cluster_wal_thread_checkpoint_sample_v1(&sample));
	UT_ASSERT_EQ(sample.root_publish_seq, 6);
	UT_ASSERT_EQ(sample.retained_lower, 100);
	UT_ASSERT_EQ(sample.native_redo, 800);
	UT_ASSERT_EQ(sample.validated_tail, 1000);
	pg_atomic_write_u32(&shared.writer_ref_state, 1);
	UT_ASSERT(!cluster_wal_thread_checkpoint_sample_v1(&sample));
	UT_ASSERT_EQ(memcmp(&sample, &zero, sizeof(sample)), 0);
	cluster_wal_thread_shmem = NULL;
	cluster_wal_thread_checkpoint_observed_v1(&record, 800);
	UT_ASSERT(!cluster_wal_thread_checkpoint_sample_v1(&sample));
	UT_ASSERT(!cluster_wal_thread_checkpoint_sample_v1(NULL));
}

typedef struct ReturnSetInfo ReturnSetInfo;
static char dump_history[40], dump_span[40], dump_flush[40], dump_root[40];
static unsigned flush_reads;
static XLogRecPtr dump_native_flush;
static TimeLineID dump_native_tli;
uint64
cluster_wal_thread_page_stamp_count(void)
{
	return 0;
}
uint16
cluster_wal_thread_dump_thread_id(void)
{
	return 2;
}
bool
cluster_wal_thread_dir_configured(void)
{
	return true;
}
bool
cluster_wal_thread_dir_validated(void)
{
	return true;
}
bool
cluster_wal_thread_claim_created(void)
{
	return false;
}
bool
cluster_wal_state_registry_ready(void)
{
	return false;
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 id, ClusterWalStateSlot *out)
{
	abort();
}
XLogRecPtr
GetFlushRecPtr(TimeLineID *timeline)
{
	flush_reads++;
	*timeline = dump_native_tli;
	return dump_native_flush;
}
static const char *
fmt_uint64(uint64 value)
{
	static char text[40];
	snprintf(text, sizeof(text), UINT64_FORMAT, value);
	return text;
}
static const char *
fmt_int64(int64 value)
{
	return fmt_uint64((uint64)value);
}
static const char *
fmt_int32(int32 value)
{
	return fmt_int64(value);
}
static const char *
fmt_uint64_hex(uint64 value)
{
	return fmt_uint64(value);
}
static const char *
fmt_bool(bool value)
{
	return value ? "t" : "f";
}
static void
emit_row(ReturnSetInfo *unused, const char *category, const char *key, const char *value)
{
	char *target = NULL;
	UT_ASSERT(strcmp(category, "wal_thread") == 0);
	if (strcmp(key, "observed_retained_history_bytes") == 0)
		target = dump_history;
	if (strcmp(key, "observed_to_flush_retained_bytes") == 0)
		target = dump_span;
	if (strcmp(key, "native_flush_lsn") == 0)
		target = dump_flush;
	if (strcmp(key, "observed_root_publish_seq") == 0)
		target = dump_root;
	if (target != NULL)
		strlcpy(target, value, 40);
}
#include "test_cluster_wal_checkpoint_dump.inc"

UT_TEST(actual_dump_distinguishes_unknown_history_and_live_flush_span)
{
	ClusterControlRootSnapshot record = reset_sample();
	flush_reads = 0;
	dump_native_tli = 1;
	dump_native_flush = 900;
	dump_wal_thread(NULL);
	UT_ASSERT(strcmp(dump_history, "-") == 0 && strcmp(dump_span, "-") == 0);
	UT_ASSERT(strcmp(dump_root, "-") == 0 && strcmp(dump_flush, "900") == 0);
	cluster_wal_thread_checkpoint_observed_v1(&record, 300);
	dump_wal_thread(NULL);
	UT_ASSERT(strcmp(dump_history, "200") == 0 && strcmp(dump_span, "800") == 0);
	UT_ASSERT(strcmp(dump_root, "5") == 0);
	dump_native_flush = 499;
	dump_wal_thread(NULL);
	UT_ASSERT(strcmp(dump_history, "200") == 0 && strcmp(dump_span, "-") == 0);
	dump_native_tli = 2;
	dump_wal_thread(NULL);
	UT_ASSERT(strcmp(dump_flush, "-") == 0 && strcmp(dump_span, "-") == 0);
	pg_atomic_write_u32(&shared.writer_ref_state, 1);
	dump_wal_thread(NULL);
	UT_ASSERT_EQ(flush_reads, 4);
	UT_ASSERT(strcmp(dump_history, "-") == 0 && strcmp(dump_root, "-") == 0);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(sample_is_unknown_until_exact_current_writer_checkpoint);
	UT_RUN(invalid_or_old_checkpoint_never_replaces_last_observation);
	UT_RUN(new_checkpoint_replaces_whole_sample_and_unbound_reader_gets_unknown);
	UT_RUN(actual_dump_distinguishes_unknown_history_and_live_flush_span);
	UT_DONE();
	return ut_failed_count != 0;
}
