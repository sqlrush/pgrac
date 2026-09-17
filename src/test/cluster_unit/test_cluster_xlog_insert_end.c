/*-------------------------------------------------------------------------
 *
 * test_cluster_xlog_insert_end.c
 *	  M7 record-end WAL flush-ceiling regression tests.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <pthread.h>
#include <stdlib.h>

#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "port/atomics.h"
#include "storage/spin.h"

#undef printf
#undef fprintf

#include "unit_test.h"

#ifndef BUFMGR_SOURCE_PATH
#error "BUFMGR_SOURCE_PATH must identify bufmgr.c"
#endif

UT_DEFINE_GLOBALS();

void
ExceptionalCondition(const char *condition_name, const char *file_name, int line_number)
{
	(void)condition_name;
	(void)file_name;
	(void)line_number;
	abort();
}

/* Slow path used when the production spinlock observes test-thread contention. */
int
s_lock(volatile slock_t *lock, const char *file pg_attribute_unused(),
	   int line pg_attribute_unused(), const char *func pg_attribute_unused())
{
	while (TAS_SPIN(lock))
		;
	return 0;
}

/* Defined only by the USE_CLUSTER_UNIT build of the production xlog source. */
extern void cluster_xlog_unit_init_insert_bytepos(uint64 bytepos);
extern void cluster_xlog_unit_set_insert_bytepos(uint64 bytepos);

static uint64
usable_bytes_in_segment(void)
{
	return ((uint64)wal_segment_size / XLOG_BLCKSZ) * (XLOG_BLCKSZ - SizeOfXLogShortPHD)
		   - (SizeOfXLogLongPHD - SizeOfXLogShortPHD);
}

static uint64
second_internal_page_end_bytepos(void)
{
	return (XLOG_BLCKSZ - SizeOfXLogLongPHD) + (XLOG_BLCKSZ - SizeOfXLogShortPHD);
}

UT_TEST(test_internal_page_boundary_returns_record_end_not_next_start)
{
	uint64 bytepos = second_internal_page_end_bytepos();
	XLogRecPtr boundary = (XLogRecPtr)2 * XLOG_BLCKSZ;

	cluster_xlog_unit_init_insert_bytepos(bytepos);
	UT_ASSERT_EQ(GetXLogInsertEndRecPtr(), boundary);
	UT_ASSERT_NE(GetXLogInsertEndRecPtr(), boundary + SizeOfXLogShortPHD);
}

UT_TEST(test_segment_boundary_returns_boundary_not_long_header_start)
{
	XLogRecPtr boundary = (XLogRecPtr)wal_segment_size;

	cluster_xlog_unit_init_insert_bytepos(usable_bytes_in_segment());
	UT_ASSERT_EQ(GetXLogInsertEndRecPtr(), boundary);
	UT_ASSERT_NE(GetXLogInsertEndRecPtr(), boundary + SizeOfXLogLongPHD);
}

typedef struct InsertEndRace {
	uint64 bytepos_a;
	uint64 bytepos_b;
	XLogRecPtr expected_a;
	XLogRecPtr expected_b;
	pg_atomic_uint32 start;
	pg_atomic_uint32 published;
	pg_atomic_uint32 sampled;
	pg_atomic_uint32 stop;
	pg_atomic_uint32 bad;
} InsertEndRace;

static void *
insert_end_writer(void *arg)
{
	InsertEndRace *race = (InsertEndRace *)arg;
	int i;

	while (pg_atomic_read_u32(&race->start) == 0)
		;
	cluster_xlog_unit_set_insert_bytepos(race->bytepos_b);
	pg_atomic_write_u32(&race->published, 1);
	while (pg_atomic_read_u32(&race->sampled) == 0)
		;
	for (i = 0; i < 100000; i++)
		cluster_xlog_unit_set_insert_bytepos((i & 1) ? race->bytepos_a : race->bytepos_b);
	pg_atomic_write_u32(&race->stop, 1);
	return NULL;
}

UT_TEST(test_concurrent_reservation_sample_is_one_record_end)
{
	InsertEndRace race;
	pthread_t writer;
	uint64 samples = 0;
	int rc;

	race.bytepos_a = second_internal_page_end_bytepos();
	race.bytepos_b = usable_bytes_in_segment();
	race.expected_a = (XLogRecPtr)2 * XLOG_BLCKSZ;
	race.expected_b = (XLogRecPtr)wal_segment_size;
	pg_atomic_init_u32(&race.start, 0);
	pg_atomic_init_u32(&race.published, 0);
	pg_atomic_init_u32(&race.sampled, 0);
	pg_atomic_init_u32(&race.stop, 0);
	pg_atomic_init_u32(&race.bad, 0);
	cluster_xlog_unit_init_insert_bytepos(race.bytepos_a);

	rc = pthread_create(&writer, NULL, insert_end_writer, &race);
	UT_ASSERT_EQ(rc, 0);
	if (rc != 0)
		return;
	pg_atomic_write_u32(&race.start, 1);
	while (pg_atomic_read_u32(&race.published) == 0)
		;
	while (pg_atomic_read_u32(&race.stop) == 0) {
		XLogRecPtr sample = GetXLogInsertEndRecPtr();

		samples++;
		pg_atomic_write_u32(&race.sampled, 1);
		if (sample != race.expected_a && sample != race.expected_b)
			pg_atomic_write_u32(&race.bad, 1);
	}
	rc = pthread_join(writer, NULL);
	UT_ASSERT_EQ(rc, 0);
	UT_ASSERT(samples > 0);
	UT_ASSERT_EQ(pg_atomic_read_u32(&race.bad), 0);
}

static char *
read_bufmgr_source(void)
{
	FILE *file;
	long length;
	char *source;

	file = fopen(BUFMGR_SOURCE_PATH, "rb");
	UT_ASSERT_NOT_NULL(file);
	if (file == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(file, 0, SEEK_END), 0);
	length = ftell(file);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(file, 0, SEEK_SET), 0);
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(file);
		return NULL;
	}
	UT_ASSERT_EQ(fread(source, 1, (size_t)length, file), (size_t)length);
	source[length] = '\0';
	fclose(file);
	return source;
}

UT_TEST(test_ship_flush_ceiling_uses_record_end_accessor)
{
	char *source = read_bufmgr_source();
	const char *begin;
	const char *end;

	if (source == NULL)
		return;
	begin = strstr(source, "\ncluster_gcs_clamp_ship_flush_lsn(");
	UT_ASSERT_NOT_NULL(begin);
	end = begin != NULL ? strstr(begin, "\n}\n") : NULL;
	UT_ASSERT_NOT_NULL(end);
	if (begin != NULL && end != NULL) {
		const char *record_end = strstr(begin, "GetXLogInsertEndRecPtr()");
		const char *next_start = strstr(begin, "GetXLogInsertRecPtr()");

		UT_ASSERT_NOT_NULL(record_end);
		UT_ASSERT(record_end == NULL || record_end < end);
		UT_ASSERT(next_start == NULL || next_start > end);
	}
	free(source);
}

int
main(void)
{
	UT_PLAN(4);
	UT_RUN(test_internal_page_boundary_returns_record_end_not_next_start);
	UT_RUN(test_segment_boundary_returns_boundary_not_long_header_start);
	UT_RUN(test_concurrent_reservation_sample_is_one_record_end);
	UT_RUN(test_ship_flush_ceiling_uses_record_end_accessor);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
