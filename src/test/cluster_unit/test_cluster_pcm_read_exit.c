/* Native exit-hook registration around the first-read cleanup bracket.
 * Author: SqlRush <sqlrush@gmail.com>
 */
#include "postgres.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_gcs_block_dedup.h"
#include "cluster/cluster_ko.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "utils/hsearch.h"

#undef printf
#undef fprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

static sigjmp_buf fixture_error_stack;
static bool fixture_catching;
static void fixture_error(void) pg_attribute_noreturn();
static void
fixture_error(void)
{
	if (fixture_catching)
		siglongjmp(fixture_error_stack, 1);
	abort();
}
#undef ereport
#undef elog
#define ereport(level, rest) fixture_error()
#define elog(...) fixture_error()

/* The included ipc.c owners operate on their native callback stack. */
#define MAX_ON_EXITS 20
struct ONEXIT { pg_on_exit_callback function; Datum arg; };
static struct ONEXIT before_shmem_exit_list[MAX_ON_EXITS];
static int before_shmem_exit_index;
static bool atexit_callback_setup = true;
static bool dedup_backend_exit_hook_registered;
static void atexit_callback(void) { }

bool IsUnderPostmaster = true;
BackendId MyBackendId;
int cluster_node_id = 2;
PGPROC *MyProc;
static PGPROC fixture_proc;
static BufferDescPadded fixture_buffers[1];
BufferDescPadded *BufferDescriptors = fixture_buffers;
static bool fixture_quiet;
static unsigned fixture_nacks, fixture_dedup_retirements;
static ClusterPcmReadGrantKey fixture_retired_key;

/* Hash allocation and transport are boundaries; callback selection, ledger
 * lifetime, native LIFO cancellation and startup wiring execute real bodies. */
typedef struct PrivateRefCountEntry {
	Buffer buffer;
	int32 refcount;
	ClusterKoCurrentAdmissionV3 ko_current;
} PrivateRefCountEntry;
static PrivateRefCountEntry PrivateRefCountArray[8];
static HTAB *PrivateRefCountHash;
static int fixture_hash;
HTAB *
hash_create(const char *name pg_attribute_unused(), long count pg_attribute_unused(),
			const HASHCTL *ctl pg_attribute_unused(), int flags pg_attribute_unused())
{
	return (HTAB *)&fixture_hash;
}
static void cluster_bufmgr_pcm_x_writer_reset(void) { }
static void AtProcExit_Buffers(int code pg_attribute_unused(), Datum arg pg_attribute_unused()) { }
void
on_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	UT_ASSERT(callback == AtProcExit_Buffers);
	UT_ASSERT_EQ(arg, 0);
}
void
cluster_gcs_block_dedup_cleanup_on_backend_exit(uint32 node, int32 backend)
{
	UT_ASSERT_EQ(node, 2);
	UT_ASSERT_EQ(backend, 7);
	UT_ASSERT(MyProc != NULL);
	UT_ASSERT_EQ(fixture_nacks, 1);
	fixture_dedup_retirements++;
}
bool
cluster_gcs_block_read_source_quiesce(BufferDesc *buf, const ClusterPcmReadSource *source)
{
	UT_ASSERT(buf == &fixture_buffers[0].bufferdesc);
	UT_ASSERT_EQ(source->request_id, 41);
	UT_ASSERT(MyProc != NULL);
	return fixture_quiet;
}
static void
gcs_block_pending_s_notify(const ClusterPcmReadGrantKey *key, int master, bool installed)
{
	UT_ASSERT_EQ(master, 1);
	UT_ASSERT(!installed);
	fixture_retired_key = *key;
	fixture_nacks++;
}
void
ExceptionalCondition(const char *condition pg_attribute_unused(),
					 const char *file pg_attribute_unused(), int line pg_attribute_unused())
{
	abort();
}

#include "test_cluster_pcm_read_exit.inc"

static void
reset_fixture(void)
{
	before_shmem_exit_index = 0;
	dedup_backend_exit_hook_registered = false;
	gcs_read_source_exit_registered = false;
	memset(gcs_read_source_attempts, 0, sizeof(gcs_read_source_attempts));
	memset(fixture_buffers, 0, sizeof(fixture_buffers));
	MyProc = &fixture_proc;
	MyBackendId = InvalidBackendId;
	fixture_catching = false;
	fixture_quiet = true;
	fixture_nacks = fixture_dedup_retirements = 0;
}
static ClusterPcmReadSource
make_source(void)
{
	ClusterPcmReadSource source = { 0 };
	source.tag.relNumber = 16392;
	source.tag.blockNum = 94;
	source.base_generation = 48;
	source.reservation_token = 49;
	source.request_id = 41;
	source.request_epoch = 3;
	source.master_node = 1;
	source.pending_s = true;
	return source;
}
static void temporary_cleanup(int code pg_attribute_unused(), Datum arg pg_attribute_unused()) { }

UT_TEST(test_startup_installs_persistent_owners_before_backend_id)
{
	reset_fixture();
	InitBufferPoolAccess();
	UT_ASSERT_EQ(before_shmem_exit_index, 2);
	MyBackendId = 7;
	InitBufferPoolAccess();
	UT_ASSERT_EQ(before_shmem_exit_index, 2);
}

UT_TEST(test_first_source_does_not_displace_nested_native_cleanup)
{
	ClusterPcmReadSource source = make_source();
	volatile bool mismatch = false;
	reset_fixture();
	InitBufferPoolAccess();
	MyBackendId = 7;
	before_shmem_exit(temporary_cleanup, (Datum)1);
	before_shmem_exit(temporary_cleanup, (Datum)2);
	cluster_gcs_block_dedup_register_backend_exit_hook();
	UT_ASSERT(gcs_read_source_arm(&fixture_buffers[0].bufferdesc, &source));
	fixture_catching = true;
	if (sigsetjmp(fixture_error_stack, 0) == 0) {
		cancel_before_shmem_exit(temporary_cleanup, (Datum)2);
		cancel_before_shmem_exit(temporary_cleanup, (Datum)1);
	} else
		mismatch = true;
	fixture_catching = false;
	UT_ASSERT(!mismatch);
	UT_ASSERT_EQ(before_shmem_exit_index, 2);
}

UT_TEST(test_exit_retires_exact_source_before_low_level_resources)
{
	ClusterPcmReadSource source = make_source();
	reset_fixture();
	InitBufferPoolAccess();
	MyBackendId = 7;
	UT_ASSERT(gcs_read_source_arm(&fixture_buffers[0].bufferdesc, &source));
	while (before_shmem_exit_index > 0) {
		struct ONEXIT hook = before_shmem_exit_list[--before_shmem_exit_index];
		hook.function(0, hook.arg);
	}
	UT_ASSERT_EQ(fixture_nacks, 1);
	UT_ASSERT_EQ(fixture_retired_key.request_id, 41);
	UT_ASSERT_EQ(fixture_retired_key.epoch, 3);
	UT_ASSERT_EQ(fixture_retired_key.requester_node, 2);
	UT_ASSERT_EQ(fixture_retired_key.backend_id, 7);
	UT_ASSERT(BufferTagsEqual(&fixture_retired_key.tag, &source.tag));
	UT_ASSERT(!gcs_read_source_attempts[0].used);
	UT_ASSERT_EQ(fixture_dedup_retirements, 1);
}

UT_TEST(test_unproven_delivery_retains_source_debt)
{
	ClusterPcmReadSource source = make_source();
	reset_fixture();
	InitBufferPoolAccess();
	MyBackendId = 7;
	fixture_quiet = false;
	UT_ASSERT(gcs_read_source_arm(&fixture_buffers[0].bufferdesc, &source));
	gcs_read_source_backend_exit(0, (Datum)0);
	UT_ASSERT_EQ(fixture_nacks, 0);
	UT_ASSERT(gcs_read_source_attempts[0].used);
}

UT_TEST(test_uninitialized_source_cannot_add_a_late_exit_owner)
{
	ClusterPcmReadSource source = make_source();
	volatile bool refused = false;
	reset_fixture();
	fixture_catching = true;
	if (sigsetjmp(fixture_error_stack, 0) == 0)
		(void)gcs_read_source_arm(&fixture_buffers[0].bufferdesc, &source);
	else
		refused = true;
	fixture_catching = false;
	UT_ASSERT(refused);
	UT_ASSERT_EQ(before_shmem_exit_index, 0);
	UT_ASSERT(!gcs_read_source_attempts[0].used);
}

UT_TEST(test_auxiliary_exit_without_backend_id_has_no_dedup_identity)
{
	reset_fixture();
	InitBufferPoolAccess();
	while (before_shmem_exit_index > 0) {
		struct ONEXIT hook = before_shmem_exit_list[--before_shmem_exit_index];
		hook.function(0, hook.arg);
	}
	UT_ASSERT_EQ(fixture_nacks, 0);
	UT_ASSERT_EQ(fixture_dedup_retirements, 0);
}

int
main(void)
{
	UT_PLAN(6);
	UT_RUN(test_startup_installs_persistent_owners_before_backend_id);
	UT_RUN(test_first_source_does_not_displace_nested_native_cleanup);
	UT_RUN(test_exit_retires_exact_source_before_low_level_resources);
	UT_RUN(test_unproven_delivery_retains_source_debt);
	UT_RUN(test_uninitialized_source_cannot_add_a_late_exit_owner);
	UT_RUN(test_auxiliary_exit_without_backend_id_has_no_dedup_identity);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
