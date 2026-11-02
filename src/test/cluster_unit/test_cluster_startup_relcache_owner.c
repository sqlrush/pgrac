/* Pre-transaction shared catalog initialization with backend crypto resources.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#include "postgres.h"
#include <sys/resource.h>
#include "utils/memutils.h"
#include "utils/relcache.h"

/* Retain the original real-file CF fixture and its default tests unchanged. */
int startup_owner_original_cf_main(void);
bool startup_owner_original_errstart(int elevel, const char *domain);
bool startup_owner_original_errstart_cold(int elevel, const char *domain);
void startup_owner_original_errfinish(const char *file, int line, const char *func);
#define main startup_owner_original_cf_main
#define errstart startup_owner_original_errstart
#define errstart_cold startup_owner_original_errstart_cold
#define errfinish startup_owner_original_errfinish
#include "test_cluster_cf_authority.c"
#undef main
#undef errstart
#undef errstart_cold
#undef errfinish

/* Complete original owner implementation; no permissive crypto-owner stubs. */
#include "../../backend/utils/resowner/resowner.c"

static MemoryContextData startup_context = { .type = T_AllocSetContext };
static MemoryContextData failure_context = { .type = T_AllocSetContext };
MemoryContext TopMemoryContext = &startup_context;
MemoryContext CurrentMemoryContext = &startup_context;
sigjmp_buf *PG_exception_stack;
ErrorContextCallback *error_context_stack;
static unsigned allocations;
static bool fail_allocation;
static unsigned release_phases;
static ResourceOwner expected_owner;
static ControlFileData original_control;
static ClusterCfImageStage original_stage;
static unsigned phase_mode;

void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	void *p;
	if (fail_allocation) {
		fail_allocation = false;
		pg_re_throw();
	}
	p = malloc(size);
	if (!p) abort();
	allocations++;
	return p;
}

void *
MemoryContextAllocZero(MemoryContext context, Size size)
{
	void *p = MemoryContextAlloc(context, size);
	memset(p, 0, size);
	return p;
}

void
pfree(void *p)
{
	if (!p || allocations == 0) abort();
	allocations--;
	free(p);
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	return elevel >= ERROR;
}
bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{
	pg_re_throw();
}
void
pg_re_throw(void)
{
	if (!PG_exception_stack) abort();
	siglongjmp(*PG_exception_stack, 1);
}

/* These resource kinds must never be acquired by this early read. Any
 * unexpected cleanup is a hard failure, rather than a fake successful release. */
void AbortBufferIO(Buffer b pg_attribute_unused()) { abort(); }
void ReleaseBuffer(Buffer b pg_attribute_unused()) { abort(); }
void PrintBufferLeakWarning(Buffer b pg_attribute_unused()) { abort(); }
void RelationClose(Relation r pg_attribute_unused()) { abort(); }
void dsm_detach(dsm_segment *s pg_attribute_unused()) { abort(); }
dsm_handle dsm_segment_handle(dsm_segment *s pg_attribute_unused()) { abort(); }
void jit_release_context(JitContext *c pg_attribute_unused()) { abort(); }
void pg_hmac_free(pg_hmac_ctx *c pg_attribute_unused()) { abort(); }
void ProcReleaseLocks(bool commit pg_attribute_unused()) { abort(); }
void ReleasePredicateLocks(bool commit pg_attribute_unused(), bool partial pg_attribute_unused()) { abort(); }
void LockReassignCurrentOwner(LOCALLOCK **l pg_attribute_unused(), int n pg_attribute_unused()) { abort(); }
void LockReleaseCurrentOwner(LOCALLOCK **l pg_attribute_unused(), int n pg_attribute_unused()) { abort(); }
void ReleaseCatCache(HeapTuple t pg_attribute_unused()) { abort(); }
void PrintCatCacheLeakWarning(HeapTuple t pg_attribute_unused()) { abort(); }
void ReleaseCatCacheList(CatCList *l pg_attribute_unused()) { abort(); }
void PrintCatCacheListLeakWarning(CatCList *l pg_attribute_unused()) { abort(); }
void ReleaseCachedPlan(CachedPlan *p pg_attribute_unused(), ResourceOwner o pg_attribute_unused()) { abort(); }
void DecrTupleDescRefCount(TupleDesc d pg_attribute_unused()) { abort(); }
void UnregisterSnapshot(Snapshot s pg_attribute_unused()) { abort(); }
void FileClose(File f pg_attribute_unused()) { abort(); }

static void
phase_release(ResourceReleasePhase phase, bool commit pg_attribute_unused(),
			  bool top, void *arg pg_attribute_unused())
{
	UT_ASSERT(CurrentResourceOwner != NULL);
	UT_ASSERT(CurrentResourceOwner != expected_owner);
	UT_ASSERT(top);
	release_phases |= 1U << phase;
}

/* Original Phase2's control read runs here with the caller's actual owner.
 * The rest of relcache construction is not modeled as native qualification. */
void
RelationCacheInitializePhase2(void)
{
	ControlFileData out;
	ClusterControlRootResult result;
	pg_cryptohash_ctx *hash;
	if (phase_mode == 3) {
		UT_ASSERT(CurrentResourceOwner == expected_owner);
		return;
	}
	if (phase_mode == 1) {
		hash = pg_cryptohash_create(PG_SHA256);
		UT_ASSERT(hash != NULL);
		UT_ASSERT_EQ(CurrentResourceOwner->cryptohasharr.nitems, 1);
		CurrentMemoryContext = &failure_context;
		pg_re_throw(); /* the real owner must free this unfinished EVP context */
	}
	if (phase_mode == 2) fail_allocation = true;
	result = cluster_cf_control_image_read_locked(original_stage.generation,
		original_stage.image_sha256, original_stage.system_identifier, &out);
	if (phase_mode == 4) {
		ControlFileData zero = { 0 };
		UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_HASH_MISMATCH);
		UT_ASSERT_EQ(memcmp(&out, &zero, sizeof(out)), 0);
		return;
	}
	UT_ASSERT_EQ(result, CLUSTER_CONTROL_ROOT_OK_PRIMARY);
	UT_ASSERT_EQ(memcmp(&out, &original_control, sizeof(out)), 0);
	UT_ASSERT(CurrentResourceOwner != NULL);
	if (expected_owner) UT_ASSERT(CurrentResourceOwner == expected_owner);
	UT_ASSERT_EQ(CurrentResourceOwner->cryptohasharr.nitems, 0);
}

static void
initialize_shared_catalogs(void)
{
#include "test_cluster_startup_relcache_owner.inc"
}

static void
run_case(unsigned mode)
{
	ResourceOwner preparation;
	uint8 uuid[16] = { 0x91 };
	uint8 canonical[PG_CONTROL_FILE_SIZE];
	unsigned before;
	volatile bool caught = false;
	bool shared = mode != 4;

	phase_mode = 0;
	setup_shared_root();
	image_input(&original_control);
	REQUIRE_IMAGE_OK(cluster_cf_control_image_encode(&original_control, canonical));
	memcpy(&original_control, canonical, sizeof(original_control));
	/* Prepare original immutable input using an ordinary real owner. */
	preparation = ResourceOwnerCreate(NULL, "fixture input");
	CurrentResourceOwner = preparation;
	REQUIRE_IMAGE_OK(cluster_cf_control_image_prepare(&original_control, 101, uuid, &original_stage));
	REQUIRE_IMAGE_OK(cluster_cf_control_image_install(&original_stage));
	CurrentResourceOwner = NULL;
	ResourceOwnerDelete(preparation);
	if (mode == 5) {
		char final[MAXPGPATH], temp[MAXPGPATH];
		image_paths(&original_stage, final, temp);
		flip_byte(final, 0);
	}
	before = allocations;
	if (mode == 1) {
		expected_owner = ResourceOwnerCreate(NULL, "original caller");
		CurrentResourceOwner = expected_owner;
	}
	RegisterResourceReleaseCallback(phase_release, NULL);
	cluster_shared_config = shared;
	phase_mode = mode == 2 ? 1 : mode == 3 ? 2 : mode == 4 ? 3 : mode == 5 ? 4 : 0;
	PG_TRY();
	{
		initialize_shared_catalogs();
	}
	PG_CATCH();
	{
		caught = true;
	}
	PG_END_TRY();
	UT_ASSERT(caught == (mode == 2 || mode == 3));
	UT_ASSERT(CurrentResourceOwner == expected_owner);
	UT_ASSERT(CurrentMemoryContext == &startup_context);
	UT_ASSERT_EQ(release_phases, shared && mode != 1 ? 7 : 0);
	UnregisterResourceReleaseCallback(phase_release, NULL);
	if (expected_owner) {
		UT_ASSERT_EQ(expected_owner->cryptohasharr.nitems, 0);
		CurrentResourceOwner = NULL;
		ResourceOwnerDelete(expected_owner);
		expected_owner = NULL;
	}
	UT_ASSERT_EQ(allocations, before);
	UT_ASSERT(image_cf_global && image_cf_x); /* original CF custody unchanged */
	UT_ASSERT_EQ(cluster_cf_control_image_discard(&original_stage), 0);
}

static void
run_child(unsigned mode)
{
	pid_t child = fork();
	int status;
	UT_ASSERT(child >= 0);
	if (child == 0) {
		struct rlimit limit = { 0, 0 };
		(void)setrlimit(RLIMIT_CORE, &limit);
		run_case(mode);
		fflush(stdout);
		_exit(ut_current_failed ? 1 : 0);
	}
	UT_ASSERT_EQ(waitpid(child, &status, 0), child);
	if (WIFSIGNALED(status))
		printf("# child signal=%d\n", WTERMSIG(status));
	UT_ASSERT(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

int
main(void)
{
	UT_PLAN(6);
	for (unsigned mode = 0; mode < 6; mode++) {
		ut_current_failed = 0;
		fflush(stdout);
		run_child(mode);
		ut_test_count++;
		if (ut_current_failed) ut_failed_count++;
		printf("%s %u - startup owner case %u\n", ut_current_failed ? "not ok" : "ok",
			ut_test_count, mode);
	}
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
