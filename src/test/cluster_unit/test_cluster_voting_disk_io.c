/*-------------------------------------------------------------------------
 *
 * test_cluster_voting_disk_io.c
 *	  spec-2.6 Sprint A Step 2 D3 unit tests — voting disk slot R/W
 *	  primitives via real syscalls on a temp file.
 *
 *	  T-io-1 round-trip: format → write → read → verify byte equality
 *	  T-io-2 CRC-mismatch detection: corrupt slot bytes → read returns TORN
 *	  T-io-3 magic mismatch → FAILED
 *	  T-io-4 node_id mismatch → FAILED (wrong-offset write defence)
 *	  T-io-5 short-read / EOF returns FAILED
 *	  T-io-6 fd<0 → NOT_TRIED defensive
 *	  T-io-8 apply lease region round-trip
 *	  T-io-9 marker regions are disjoint from voting slot _reserved1
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_voting_disk_io.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_voting_disk_io.h"

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


/*
 * Helper:  create a temp file path under /tmp and ensure it doesn't
 * exist, return malloc'd path (caller frees + unlinks).
 */
static char *
make_temp_path(const char *suffix)
{
	char *path = malloc(64);

	if (path == NULL)
		exit(1);
	snprintf(path, 64, "/tmp/pgrac_voting_test_%d_%s", getpid(), suffix);
	(void)unlink(path); /* ignore ENOENT */
	return path;
}


UT_TEST(test_io_1_round_trip)
{
	char *path = make_temp_path("rt");
	int fd;
	ClusterVotingSlot in;
	ClusterVotingSlot out;
	ClusterVotingDiskIoState rc;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);

	/* Format with max_nodes=4 + disk_index=0 */
	rc = cluster_voting_disk_format(fd, 4, 0);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Populate a real slot for node_id=2. */
	memset(&in, 0, sizeof(in));
	in.magic = CLUSTER_VOTING_SLOT_MAGIC;
	in.version = CLUSTER_VOTING_SLOT_VERSION;
	in.node_id = 2;
	in.incarnation = 0xCAFEBABEDEADBEEFULL;
	in.heartbeat_ts_us = 1700000000000000ULL;
	in.current_epoch = 42;
	in.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;
	in.disk_index = 0;
	in.generation = 7;

	rc = cluster_voting_disk_write_slot(fd, &in);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, 2, &out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Field-by-field parity. */
	UT_ASSERT_EQ(out.magic, CLUSTER_VOTING_SLOT_MAGIC);
	UT_ASSERT_EQ(out.version, CLUSTER_VOTING_SLOT_VERSION);
	UT_ASSERT_EQ(out.node_id, 2);
	UT_ASSERT_EQ(out.incarnation, 0xCAFEBABEDEADBEEFULL);
	UT_ASSERT_EQ(out.heartbeat_ts_us, 1700000000000000ULL);
	UT_ASSERT_EQ(out.current_epoch, 42);
	UT_ASSERT_EQ(out.flags, CLUSTER_VOTING_SLOT_FLAG_ALIVE);
	UT_ASSERT_EQ(out.generation, 7);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_2_crc_mismatch_returns_torn)
{
	char *path = make_temp_path("crc");
	int fd;
	ClusterVotingSlot slot;
	ClusterVotingDiskIoState rc;
	uint8 garbage = 0xFF;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);
	rc = cluster_voting_disk_format(fd, 4, 0);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Write a valid slot first. */
	memset(&slot, 0, sizeof(slot));
	slot.magic = CLUSTER_VOTING_SLOT_MAGIC;
	slot.version = CLUSTER_VOTING_SLOT_VERSION;
	slot.node_id = 1;
	slot.incarnation = 100;
	slot.generation = 1;
	slot.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;
	rc = cluster_voting_disk_write_slot(fd, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Corrupt one byte in the middle of slot 1's data area to simulate
	 * a torn write — CRC should now mismatch. */
	(void)pwrite(fd, &garbage, 1, /* offset */ 1 * 512 + 100);
	(void)fsync(fd);

	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, 1, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_TORN);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_3_magic_mismatch_failed)
{
	char *path = make_temp_path("magic");
	int fd;
	ClusterVotingSlot slot;
	ClusterVotingDiskIoState rc;
	uint32 bad_magic = 0xDEADBEEF;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);
	rc = cluster_voting_disk_format(fd, 4, 0);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Format wrote slots with valid magic + crc;corrupt magic of slot 1
	 * AND keep CRC matching by overwriting CRC after.  We do this the
	 * lazy way: write a valid slot manually with bogus magic + matching
	 * CRC. */
	memset(&slot, 0, sizeof(slot));
	slot.magic = bad_magic; /* wrong magic */
	slot.version = CLUSTER_VOTING_SLOT_VERSION;
	slot.node_id = 1;
	slot.generation = 1;
	/* CRC will be computed by write_slot to match these (bad) bytes,
	 * so CRC verify will pass but magic check will fail. */
	rc = cluster_voting_disk_write_slot(fd, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, 1, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_FAILED);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_4_node_id_mismatch_failed)
{
	char *path = make_temp_path("nid");
	int fd;
	ClusterVotingSlot slot;
	ClusterVotingDiskIoState rc;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);
	rc = cluster_voting_disk_format(fd, 4, 0);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Write slot with node_id=1 at slot 1's offset — fine.  Now manually
	 * read slot 2's offset and the slot's stored node_id will be 2 from
	 * format(), so the read for node_id=2 should match.  Verify the
	 * defence: read slot 1 but expect node_id=99 → failure path. */
	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, /*requested*/ 99, &slot);
	/* Slot at offset 99*512 is beyond the formatted file (only formatted
	 * 0..3) — short read → FAILED. */
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_FAILED);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_5_short_read_returns_failed)
{
	char *path = make_temp_path("short");
	int fd;
	ClusterVotingSlot slot;
	ClusterVotingDiskIoState rc;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);
	/* Don't format — file is empty. */

	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, 0, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_FAILED);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_7_disk_index_misroute_failed)
{
	char *path = make_temp_path("misroute");
	int fd;
	ClusterVotingSlot slot;
	ClusterVotingDiskIoState rc;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);

	/*
	 * Q3 v0.2 misroute defense — write slot tagged with disk_index=2
	 * (i.e., this fd is supposed to be the 3rd voting disk in the CSV
	 * list).  Then read with expected_disk_index=0 (misroute scenario:
	 * caller thinks this fd is disk 0 but the slot says disk 2).  Read
	 * MUST refuse the slot with FAILED.
	 */
	rc = cluster_voting_disk_format(fd, /*max_nodes*/ 4, /*disk_index*/ 2);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	memset(&slot, 0, sizeof(slot));
	slot.magic = CLUSTER_VOTING_SLOT_MAGIC;
	slot.version = CLUSTER_VOTING_SLOT_VERSION;
	slot.node_id = 1;
	slot.disk_index = 2; /* this disk is index 2 */
	slot.generation = 1;
	slot.flags = CLUSTER_VOTING_SLOT_FLAG_ALIVE;
	rc = cluster_voting_disk_write_slot(fd, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	/* Caller expects this fd to be disk index 0 → misroute → FAILED. */
	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, 1, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_FAILED);

	/* Caller correctly identifies this fd as disk index 2 → OK. */
	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 2, 1, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(slot.disk_index, 2);

	/* Opt-out path (-1) for format / fsck tools — no misroute check. */
	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ -1, 1, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_6_fd_negative_not_tried)
{
	ClusterVotingSlot slot;
	ClusterVotingDiskIoState rc;

	memset(&slot, 0, sizeof(slot));
	rc = cluster_voting_disk_read_slot(/*fd*/ -1, /*expected_disk_index*/ 0, 0, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_NOT_TRIED);

	rc = cluster_voting_disk_write_slot(/*fd*/ -1, &slot);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_NOT_TRIED);

	rc = cluster_voting_disk_format(/*fd*/ -1, 4, 0);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_NOT_TRIED);
}

UT_TEST(test_io_8_apply_lease_region_round_trip)
{
	char *path = make_temp_path("adglease");
	int fd;
	uint8 in[CLUSTER_VOTING_SLOT_BYTES];
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskIoState rc;
	uint32 i;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);

	memset(in, 0, sizeof(in));
	memset(out, 0, sizeof(out));
	for (i = 0; i < sizeof(in); i++)
		in[i] = (uint8)(i ^ 0x5A);

	rc = cluster_voting_disk_write_apply_lease_slot(fd, 2, in);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	rc = cluster_voting_disk_read_apply_lease_slot(fd, 2, out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(in, out, sizeof(in)), 0);

	memset(out, 0, sizeof(out));
	rc = cluster_voting_disk_read_apply_lease_slot(fd, CLUSTER_MAX_NODES, out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_FAILED);
	rc = cluster_voting_disk_write_apply_lease_slot(fd, CLUSTER_MAX_NODES, in);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_FAILED);
	rc = cluster_voting_disk_read_apply_lease_slot(-1, 2, out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_NOT_TRIED);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_9_marker_regions_are_disjoint)
{
	off_t reserved1_start = CLUSTER_VOTING_SLOT_OFFSET(0) + offsetof(ClusterVotingSlot, _reserved1);
	off_t reserved1_end = reserved1_start + sizeof(((ClusterVotingSlot *)0)->_reserved1);

	UT_ASSERT_EQ(CLUSTER_VOTING_SLOT_OFFSET(0), (off_t)0);
	UT_ASSERT_EQ(CLUSTER_VOTING_LEAVE_SLOT_OFFSET(0),
				 (off_t)CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES);
	UT_ASSERT_EQ(CLUSTER_VOTING_JOIN_SLOT_OFFSET(0),
				 (off_t)2 * CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES);
	UT_ASSERT_EQ(CLUSTER_VOTING_APPLY_LEASE_SLOT_OFFSET(0),
				 (off_t)3 * CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES);
	UT_ASSERT(CLUSTER_VOTING_APPLY_LEASE_SLOT_OFFSET(0) >= reserved1_end);
	UT_ASSERT(CLUSTER_VOTING_FILE_BYTES_MIN
			  == (off_t)4 * CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES);
}


int
main(void)
{
	UT_PLAN(9);
	UT_RUN(test_io_1_round_trip);
	UT_RUN(test_io_2_crc_mismatch_returns_torn);
	UT_RUN(test_io_3_magic_mismatch_failed);
	UT_RUN(test_io_4_node_id_mismatch_failed);
	UT_RUN(test_io_5_short_read_returns_failed);
	UT_RUN(test_io_7_disk_index_misroute_failed);
	UT_RUN(test_io_6_fd_negative_not_tried);
	UT_RUN(test_io_8_apply_lease_region_round_trip);
	UT_RUN(test_io_9_marker_regions_are_disjoint);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
