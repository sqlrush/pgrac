/*-------------------------------------------------------------------------
 * test_cluster_grd_dsa.c
 *   Native in-place DSA boundary used by the bounded GRD slot pool.
 *
 * Real DSA and FreePageManager code runs here. Only process allocation,
 * uncontended LWLocks and the prohibited DSM boundary are substituted.
 * This checks capacity/lifetime, not interprocess lock scheduling.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "utils/dsa.h"
#include "utils/memutils.h"

#undef printf
#undef fprintf
#undef snprintf
#include "unit_test.h"

static LWLock *held[16];
static int nheld;
static unsigned dsm_calls;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "%s:%d: %s\n", file, line, condition);
	abort();
}
void *
palloc(Size size)
{
	void *p = malloc(size);
	if (p == NULL)
		abort();
	return p;
}
void
pfree(void *p)
{
	free(p);
}
void *
repalloc(void *p, Size size)
{
	void *n = realloc(p, size);
	if (n == NULL)
		abort();
	return n;
}
void
check_stack_depth(void)
{}
bool
errstart(int level, const char *domain)
{
	(void)level;
	(void)domain;
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errcode(int code)
{
	return code;
}
int
errmsg(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
int
errmsg_internal(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
int
errdetail(const char *fmt, ...)
{
	(void)fmt;
	return 0;
}
void
errfinish(const char *file, int line, const char *fn)
{
	fprintf(stderr, "%s:%d: unexpected native DSA error in %s\n", file, line, fn);
	abort();
}
void
LWLockInitialize(LWLock *lock, int tranche)
{
	memset(lock, 0, sizeof(*lock));
	lock->tranche = tranche;
}
bool
LWLockHeldByMe(LWLock *lock)
{
	for (int i = 0; i < nheld; i++)
		if (held[i] == lock)
			return true;
	return false;
}
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	(void)mode;
	if (nheld == lengthof(held) || LWLockHeldByMe(lock))
		abort();
	held[nheld++] = lock;
	return true;
}
void
LWLockRelease(LWLock *lock)
{
	int i;

	for (i = 0; i < nheld && held[i] != lock; i++) {}
	if (i == nheld)
		abort();
	held[i] = held[--nheld];
}

/* A fixed in-place area must never reach any DSM producer or consumer. */
dsm_segment *
dsm_create(Size size, int flags)
{
	(void)size;
	(void)flags;
	dsm_calls++;
	abort();
}
dsm_segment *
dsm_attach(dsm_handle handle)
{
	(void)handle;
	dsm_calls++;
	abort();
}
void
dsm_detach(dsm_segment *seg)
{
	(void)seg;
	dsm_calls++;
	abort();
}
void
dsm_pin_mapping(dsm_segment *seg)
{
	(void)seg;
	dsm_calls++;
	abort();
}
void
dsm_pin_segment(dsm_segment *seg)
{
	(void)seg;
	dsm_calls++;
	abort();
}
void
dsm_unpin_segment(dsm_handle handle)
{
	(void)handle;
	dsm_calls++;
	abort();
}
void *
dsm_segment_address(dsm_segment *seg)
{
	(void)seg;
	dsm_calls++;
	abort();
}
dsm_handle
dsm_segment_handle(dsm_segment *seg)
{
	(void)seg;
	dsm_calls++;
	abort();
}
void
on_dsm_detach(dsm_segment *seg, on_dsm_detach_callback cb, Datum arg)
{
	(void)seg;
	(void)cb;
	(void)arg;
	dsm_calls++;
	abort();
}

UT_TEST(native_bounded_pool_reuses_freed_storage)
{
	Size bytes = 1024 * 1024;
	void *region = palloc(bytes);
	dsa_area *area = dsa_create_in_place(region, bytes, 1, NULL);
	dsa_pointer objects[256];
	dsa_pointer retry;
	int count = 0;

	dsa_set_size_limit(area, bytes);
	dsa_pin_mapping(area);
	for (; count < lengthof(objects); count++) {
		objects[count] = dsa_allocate_extended(area, 16384, DSA_ALLOC_NO_OOM);
		if (!DsaPointerIsValid(objects[count]))
			break;
		memset(dsa_get_address(area, objects[count]), count, 16384);
	}
	UT_ASSERT(count > 0 && count < lengthof(objects));
	UT_ASSERT(!DsaPointerIsValid(dsa_allocate_extended(area, bytes, DSA_ALLOC_NO_OOM)));
	for (int i = 0; i < count; i++)
		UT_ASSERT_EQ(((unsigned char *)dsa_get_address(area, objects[i]))[16383], i);
	dsa_free(area, objects[--count]);
	retry = dsa_allocate_extended(area, 16384, DSA_ALLOC_NO_OOM);
	UT_ASSERT(DsaPointerIsValid(retry));
	dsa_free(area, retry);
	while (count > 0)
		dsa_free(area, objects[--count]);
	dsa_detach(area);
	dsa_release_in_place(region);
	pfree(region);
	UT_ASSERT_EQ(dsm_calls, 0);
	UT_ASSERT_EQ(nheld, 0);
}

UT_TEST(native_pool_survives_attachment_turnover)
{
	Size bytes = 1024 * 1024;
	void *region = palloc(bytes);
	dsa_area *creator = dsa_create_in_place(region, bytes, 1, NULL);
	dsa_area *reader;
	dsa_pointer object;

	dsa_set_size_limit(creator, bytes);
	dsa_pin(creator);
	object = dsa_allocate_extended(creator, 65536, DSA_ALLOC_NO_OOM);
	UT_ASSERT(DsaPointerIsValid(object));
	memset(dsa_get_address(creator, object), 0x5a, 65536);
	dsa_detach(creator);
	dsa_release_in_place(region);
	for (int i = 0; i < 32; i++) {
		reader = dsa_attach_in_place(region, NULL);
		dsa_pin_mapping(reader);
		UT_ASSERT_EQ(((unsigned char *)dsa_get_address(reader, object))[65535], 0x5a);
		dsa_detach(reader);
		dsa_release_in_place(region);
	}
	reader = dsa_attach_in_place(region, NULL);
	dsa_free(reader, object);
	dsa_unpin(reader);
	dsa_detach(reader);
	dsa_release_in_place(region);
	pfree(region);
	UT_ASSERT_EQ(dsm_calls, 0);
	UT_ASSERT_EQ(nheld, 0);
}

UT_DEFINE_GLOBALS();
int
main(void)
{
	UT_PLAN(2);
	UT_RUN(native_bounded_pool_reuses_freed_storage);
	UT_RUN(native_pool_survives_attachment_turnover);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
