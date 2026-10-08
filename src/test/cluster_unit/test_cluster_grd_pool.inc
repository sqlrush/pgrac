/* PGRAC: bounded allocator boundary for real GRD state-machine tests.
 * The production pool uses PostgreSQL DSA; these tests replace allocation
 * only, retaining its NO_OOM sentinel, size limit and release accounting.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "storage/ipc.h"
#include "utils/dsa.h"
#include "utils/memutils.h"
#include "nodes/memnodes.h"
#include "storage/shmem.h"

Size
mul_size(Size left, Size right)
{
	if (right != 0 && left > SIZE_MAX / right)
		abort();
	return left * right;
}

int max_locks_per_xact = 64;
int max_prepared_xacts = 0;

static bool (*ut_grd_alloc_fail)(Size bytes, int flags);

void *
palloc_extended(Size size, int flags)
{
	void *memory;

	if (ut_grd_alloc_fail != NULL && ut_grd_alloc_fail(size, flags)) {
		if (flags & MCXT_ALLOC_NO_OOM)
			return NULL;
		pg_re_throw();
	}
	memory = malloc(size);
	if (memory == NULL && !(flags & MCXT_ALLOC_NO_OOM))
		abort();
	return memory;
}

void *
palloc(Size size)
{
	return palloc_extended(size, 0);
}
static MemoryContextData grd_test_top = { .type = T_AllocSetContext };
MemoryContext TopMemoryContext = &grd_test_top;
MemoryContext CurrentMemoryContext = &grd_test_top;

static struct {
	void *address;
	Size bytes;
} ut_grd_allocations[65536];
static bool ut_grd_pool_throw;
static Size ut_grd_pool_used;
static Size ut_grd_pool_budget;
static Size ut_grd_pool_limit;
static unsigned ut_grd_pool_hint = 1;
static union {
	uint64 align;
	char data[64];
} ut_grd_pool_region;

int
cluster_conf_declared_node_count_early(void)
{
	return 4;
}

static void
ut_grd_pool_reset(void)
{
	for (unsigned i = 1; i < lengthof(ut_grd_allocations); i++) {
		free(ut_grd_allocations[i].address);
		ut_grd_allocations[i].address = NULL;
		ut_grd_allocations[i].bytes = 0;
	}
	ut_grd_pool_throw = false;
	ut_grd_alloc_fail = NULL;
	ut_grd_pool_used = 0;
	ut_grd_pool_limit = 0;
	ut_grd_pool_hint = 1;
}

dsa_area *
dsa_create_in_place(void *place, size_t size, int tranche_id, dsm_segment *segment)
{
	(void)tranche_id;
	(void)segment;
	ut_grd_pool_budget = size;
	return (dsa_area *)place;
}

dsa_area *
dsa_attach_in_place(void *place, dsm_segment *segment)
{
	(void)segment;
	return (dsa_area *)place;
}

void
dsa_set_size_limit(dsa_area *area, size_t size)
{
	(void)area;
	if (size != ut_grd_pool_budget)
		abort(); /* The product must never lift its startup bound. */
}

void
dsa_pin(dsa_area *area)
{
	(void)area;
}
void
dsa_detach(dsa_area *area)
{
	(void)area;
}
void
dsa_pin_mapping(dsa_area *area)
{
	(void)area;
}
void
dsa_release_in_place(void *place)
{
	(void)place;
}

dsa_pointer
dsa_allocate_extended(dsa_area *area, size_t size, int flags)
{
	Size limit = ut_grd_pool_limit ? ut_grd_pool_limit : ut_grd_pool_budget;

	(void)area;
	if (ut_grd_pool_throw)
		pg_re_throw(); /* PG error/cancellation at the unlocked allocator boundary. */
	if (!(flags & DSA_ALLOC_NO_OOM))
		abort();
	if (size > limit || ut_grd_pool_used > limit - size)
		return InvalidDsaPointer;
	for (unsigned n = 1; n < lengthof(ut_grd_allocations); n++) {
		unsigned i = ut_grd_pool_hint++;

		if (ut_grd_pool_hint == lengthof(ut_grd_allocations))
			ut_grd_pool_hint = 1;
		if (ut_grd_allocations[i].address != NULL)
			continue;
		ut_grd_allocations[i].address = malloc(size);
		if (ut_grd_allocations[i].address == NULL)
			return InvalidDsaPointer;
		ut_grd_allocations[i].bytes = size;
		ut_grd_pool_used += size;
		return (dsa_pointer)i;
	}
	return InvalidDsaPointer;
}

void *
dsa_get_address(dsa_area *area, dsa_pointer pointer)
{
	(void)area;
	if (pointer == 0 || pointer >= lengthof(ut_grd_allocations)
		|| ut_grd_allocations[pointer].address == NULL)
		abort();
	return ut_grd_allocations[pointer].address;
}

void
dsa_free(dsa_area *area, dsa_pointer pointer)
{
	void *address = dsa_get_address(area, pointer);

	ut_grd_pool_used -= ut_grd_allocations[pointer].bytes;
	free(address);
	memset(&ut_grd_allocations[pointer], 0, sizeof(ut_grd_allocations[pointer]));
}

#ifndef PGRAC_GRD_EXIT_FIXTURE_EXTERNAL
static pg_on_exit_callback ut_grd_before_callbacks[64];
static Datum ut_grd_before_arguments[64];
static int ut_grd_before_count;
static int ut_grd_on_count;
static int ut_grd_exit_lifo_errors;

void
before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	if (ut_grd_before_count >= lengthof(ut_grd_before_callbacks))
		abort();
	ut_grd_before_callbacks[ut_grd_before_count] = callback;
	ut_grd_before_arguments[ut_grd_before_count++] = arg;
}

void
cancel_before_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	/* Match the native IPC stack: a temporary cleanup can cancel only the
	 * last registration, never skip a permanent callback above it. */
	if (ut_grd_before_count == 0 || ut_grd_before_callbacks[ut_grd_before_count - 1] != callback
		|| ut_grd_before_arguments[ut_grd_before_count - 1] != arg) {
		ut_grd_exit_lifo_errors++;
		if (PG_exception_stack != NULL)
			siglongjmp(*PG_exception_stack, 1);
		abort();
	}
	ut_grd_before_count--;
}

void
on_shmem_exit(pg_on_exit_callback callback, Datum arg)
{
	(void)callback;
	(void)arg;
	ut_grd_on_count++;
}
#endif
