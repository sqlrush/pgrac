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
 *	  T-io-10 fixed-tail read-state enum and invalid-input outcomes
 *	  T-io-11 fixed-tail CLEAN_EOF / SHORT / FULL syscall outcomes
 *	  T-io-12 fixed-tail write lazily extends the old file by one sector
 *	  T-io-13 fixed-tail syscall failures remain distinct from EOF/short
 *	  T-io-14 epoch-ballot lane rejects invalid proposer/input boundaries
 *	  T-io-15 epoch-ballot lanes round-trip without aliasing PGSA/other lanes
 *	  T-io-16 epoch-ballot final lane requires one complete 512-byte sector
 *	  T-io-17 format preallocates the complete frozen voting-device map
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

	/* Put a CRC-valid node-1 image at node-99's physical offset.  The read must
	 * reject the round-trip identity even though the complete device map is
	 * preallocated and the sector itself is otherwise valid. */
	memset(&slot, 0, sizeof(slot));
	slot.magic = CLUSTER_VOTING_SLOT_MAGIC;
	slot.version = CLUSTER_VOTING_SLOT_VERSION;
	slot.node_id = 1;
	slot.disk_index = 0;
	slot.crc32c = cluster_voting_disk_compute_crc32c(&slot);
	UT_ASSERT_EQ(pwrite(fd, &slot, sizeof(slot), CLUSTER_VOTING_SLOT_OFFSET(99)),
				 (ssize_t)sizeof(slot));
	rc = cluster_voting_disk_read_slot(fd, /*expected_disk_index*/ 0, /*requested*/ 99, &slot);
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
	uint8 other[CLUSTER_VOTING_SLOT_BYTES];
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	ClusterVotingDiskIoState rc;
	uint32 i;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);

	memset(in, 0, sizeof(in));
	memset(other, 0, sizeof(other));
	memset(out, 0, sizeof(out));
	for (i = 0; i < sizeof(in); i++) {
		in[i] = (uint8)(i ^ 0x5A);
		other[i] = (uint8)(i ^ 0xA5);
	}

	rc = cluster_voting_disk_write_apply_lease_global_slot(fd, in);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	rc = cluster_voting_disk_read_apply_lease_global_slot(fd, out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(in, out, sizeof(in)), 0);

	rc = cluster_voting_disk_write_apply_lease_slot(fd, 2, other);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	memset(out, 0, sizeof(out));
	rc = cluster_voting_disk_read_apply_lease_global_slot(fd, out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(in, out, sizeof(in)), 0);
	memset(out, 0, sizeof(out));
	rc = cluster_voting_disk_read_apply_lease_slot(fd, 2, out);
	UT_ASSERT_EQ(rc, CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(other, out, sizeof(other)), 0);

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
	UT_ASSERT(CLUSTER_VOTING_STRIPE_SLOT_OFFSET(0)
			  == (off_t)4 * CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES);
	UT_ASSERT(CLUSTER_VOTING_STRIPE_ACTIVATION_OFFSET
			  == (off_t)5 * CLUSTER_MAX_NODES * CLUSTER_VOTING_SLOT_BYTES);
	UT_ASSERT_EQ(CLUSTER_VOTING_PGSA_SLOT_OFFSET, (off_t)328192);
	UT_ASSERT_EQ(CLUSTER_EPOCH_BALLOT_SLOT(0), 642);
	UT_ASSERT_EQ(CLUSTER_EPOCH_BALLOT_SLOT(CLUSTER_MAX_NODES - 1), 769);
	UT_ASSERT_EQ(CLUSTER_VOTING_EPOCH_BALLOT_SLOT_OFFSET(0), (off_t)328704);
	UT_ASSERT_EQ(CLUSTER_VOTING_EPOCH_BALLOT_SLOT_OFFSET(CLUSTER_MAX_NODES - 1), (off_t)393728);
	UT_ASSERT(CLUSTER_VOTING_FILE_BYTES_MIN
			  == (off_t)(6 * CLUSTER_MAX_NODES + 2) * CLUSTER_VOTING_SLOT_BYTES);
	UT_ASSERT_EQ(CLUSTER_VOTING_FILE_BYTES_MIN, (off_t)394240);
	UT_ASSERT_EQ(CLUSTER_VOTING_PGSA_SLOT_OFFSET + CLUSTER_VOTING_SLOT_BYTES,
				 CLUSTER_VOTING_EPOCH_BALLOT_SLOT_OFFSET(0));
	UT_ASSERT_EQ(CLUSTER_VOTING_EPOCH_BALLOT_SLOT_OFFSET(CLUSTER_MAX_NODES - 1)
					 + CLUSTER_VOTING_SLOT_BYTES,
				 CLUSTER_VOTING_FILE_BYTES_MIN);
}

UT_TEST(test_io_10_raw_tail_state_and_invalid_inputs)
{
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];

	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_RAW_READ_NOT_TRIED, 0);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_RAW_READ_FULL, 1);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF, 2);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_RAW_READ_SHORT, 3);
	UT_ASSERT_EQ(CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED, 4);

	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(-1, slot),
				 CLUSTER_VOTING_DISK_RAW_READ_NOT_TRIED);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_tail_slot(-1, slot),
				 CLUSTER_VOTING_DISK_IO_NOT_TRIED);
}

UT_TEST(test_io_11_raw_tail_read_distinguishes_eof_short_and_full)
{
	char *path = make_temp_path("raw_tail_read");
	uint8 expected[CLUSTER_VOTING_SLOT_BYTES];
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	int fd;
	uint32 i;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGSA_SLOT_OFFSET), 0);

	memset(out, 0xA5, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(fd, out),
				 CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF);

	memset(expected, 0x5A, sizeof(expected));
	UT_ASSERT_EQ(pwrite(fd, expected, 127, CLUSTER_VOTING_PGSA_SLOT_OFFSET), 127);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(fd, out),
				 CLUSTER_VOTING_DISK_RAW_READ_SHORT);

	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGSA_SLOT_OFFSET), 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGSA_SLOT_OFFSET + CLUSTER_VOTING_SLOT_BYTES), 0);
	memset(expected, 0, sizeof(expected));
	memset(out, 0xA5, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(fd, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(expected, out, sizeof(expected)), 0);

	for (i = 0; i < sizeof(expected); i++)
		expected[i] = (uint8)(i ^ 0xC3);
	UT_ASSERT_EQ(pwrite(fd, expected, sizeof(expected), CLUSTER_VOTING_PGSA_SLOT_OFFSET),
				 (ssize_t)sizeof(expected));
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(fd, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(expected, out, sizeof(expected)), 0);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_12_raw_tail_write_lazily_extends_old_file)
{
	char *path = make_temp_path("raw_tail_write");
	uint8 prior[CLUSTER_VOTING_SLOT_BYTES];
	uint8 in[CLUSTER_VOTING_SLOT_BYTES];
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	struct stat st;
	int fd;
	int setup_fd;
	uint32 i;

	setup_fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(setup_fd >= 0);
	UT_ASSERT_EQ(ftruncate(setup_fd, CLUSTER_VOTING_PGSA_SLOT_OFFSET), 0);
	memset(prior, 0x6D, sizeof(prior));
	UT_ASSERT_EQ(pwrite(setup_fd, prior, sizeof(prior), CLUSTER_VOTING_STRIPE_ACTIVATION_OFFSET),
				 (ssize_t)sizeof(prior));
	UT_ASSERT_EQ(fsync(setup_fd), 0);
	(void)close(setup_fd);

	fd = cluster_voting_disk_open(path, /*create*/ false);
	UT_ASSERT(fd >= 0);
	for (i = 0; i < sizeof(in); i++)
		in[i] = (uint8)(i ^ 0x39);

	UT_ASSERT_EQ(cluster_voting_disk_write_raw_tail_slot(fd, in), CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, CLUSTER_VOTING_PGSA_SLOT_OFFSET + (off_t)CLUSTER_VOTING_SLOT_BYTES);

	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(fd, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(in, out, sizeof(in)), 0);

	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(pread(fd, out, sizeof(out), CLUSTER_VOTING_STRIPE_ACTIVATION_OFFSET),
				 (ssize_t)sizeof(out));
	UT_ASSERT_EQ(memcmp(prior, out, sizeof(prior)), 0);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_13_raw_tail_syscall_errors_are_io_failed)
{
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int pipefd[2];

	memset(slot, 0, sizeof(slot));
	UT_ASSERT_EQ(pipe(pipefd), 0);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_tail_slot(pipefd[0], slot),
				 CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_tail_slot(pipefd[1], slot),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	(void)close(pipefd[0]);
	(void)close(pipefd[1]);
}

UT_TEST(test_io_14_epoch_ballot_slot_rejects_invalid_inputs)
{
	char *path = make_temp_path("epoch_ballot_invalid");
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	struct stat before;
	struct stat after;
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_FILE_BYTES_MIN), 0);
	memset(slot, 0xA7, sizeof(slot));

	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(-1, 0, slot),
				 CLUSTER_VOTING_DISK_IO_NOT_TRIED);
	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(-1, 0, slot),
				 CLUSTER_VOTING_DISK_IO_NOT_TRIED);
	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(fd, 0, NULL),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(fd, 0, NULL),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	UT_ASSERT_EQ(fstat(fd, &before), 0);
	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(fd, CLUSTER_MAX_NODES, slot),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(fd, CLUSTER_MAX_NODES, slot),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	UT_ASSERT_EQ(fstat(fd, &after), 0);
	UT_ASSERT_EQ(after.st_size, before.st_size);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_15_epoch_ballot_slots_round_trip_without_aliasing)
{
	char *path = make_temp_path("epoch_ballot_round_trip");
	uint8 pgsa[CLUSTER_VOTING_SLOT_BYTES];
	uint8 lane0[CLUSTER_VOTING_SLOT_BYTES];
	uint8 lane1[CLUSTER_VOTING_SLOT_BYTES];
	uint8 lane_last[CLUSTER_VOTING_SLOT_BYTES];
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	uint32 i;
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_FILE_BYTES_MIN), 0);
	memset(pgsa, 0x6D, sizeof(pgsa));
	memset(lane1, 0, sizeof(lane1));
	for (i = 0; i < CLUSTER_VOTING_SLOT_BYTES; i++) {
		lane0[i] = (uint8)(i ^ 0x39);
		lane_last[i] = (uint8)(i ^ 0xC3);
	}
	UT_ASSERT_EQ(pwrite(fd, pgsa, sizeof(pgsa), CLUSTER_VOTING_PGSA_SLOT_OFFSET),
				 (ssize_t)sizeof(pgsa));

	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(fd, 0, lane0),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(cluster_voting_disk_write_epoch_ballot_slot(fd, CLUSTER_MAX_NODES - 1, lane_last),
				 CLUSTER_VOTING_DISK_IO_OK);

	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(fd, 0, out), CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(out, lane0, sizeof(out)), 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(fd, 1, out), CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(out, lane1, sizeof(out)), 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(fd, CLUSTER_MAX_NODES - 1, out),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(memcmp(out, lane_last, sizeof(out)), 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(pread(fd, out, sizeof(out), CLUSTER_VOTING_PGSA_SLOT_OFFSET),
				 (ssize_t)sizeof(out));
	UT_ASSERT_EQ(memcmp(out, pgsa, sizeof(out)), 0);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_16_epoch_ballot_last_lane_requires_full_sector)
{
	char *path = make_temp_path("epoch_ballot_short");
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_FILE_BYTES_MIN - 1), 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_epoch_ballot_slot(fd, CLUSTER_MAX_NODES - 1, out),
				 CLUSTER_VOTING_DISK_IO_FAILED);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_17_format_preallocates_complete_voting_map)
{
	char *path = make_temp_path("format_capacity");
	struct stat st;
	int fd;

	fd = cluster_voting_disk_open(path, /*create*/ true);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(cluster_voting_disk_format(fd, 4, 0), CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, CLUSTER_VOTING_FILE_BYTES_MIN);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}

UT_TEST(test_io_18_epoch_ballot_authority_rejects_fixture_file)
{
	char *path = make_temp_path("epoch_ballot_attest");
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_FILE_BYTES_MIN), 0);
	UT_ASSERT(!cluster_voting_disk_epoch_ballot_authority_attest(fd));
	UT_ASSERT(!cluster_voting_disk_epoch_ballot_authority_attest(-1));

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_19_offset_raw_slot_rejects_invalid_inputs)
{
	char *path = make_temp_path("offset_raw_invalid");
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	struct stat before;
	struct stat after;
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	memset(slot, 0xa7, sizeof(slot));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(-1, 394240, slot),
				 CLUSTER_VOTING_DISK_RAW_READ_NOT_TRIED);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(-1, 394240, slot),
				 CLUSTER_VOTING_DISK_IO_NOT_TRIED);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 394240, NULL),
				 CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(fd, 394240, NULL),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	UT_ASSERT_EQ(fstat(fd, &before), 0);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, -512, slot),
				 CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(fd, 394241, slot),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	UT_ASSERT_EQ(fstat(fd, &after), 0);
	UT_ASSERT_EQ(after.st_size, before.st_size);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_20_pgrd_offsets_round_trip_without_aliasing)
{
	char *path = make_temp_path("pgrd_offsets");
	uint8 shared[CLUSTER_VOTING_SLOT_BYTES];
	uint8 local0[CLUSTER_VOTING_SLOT_BYTES];
	uint8 local127[CLUSTER_VOTING_SLOT_BYTES];
	uint8 out[CLUSTER_VOTING_SLOT_BYTES];
	struct stat st;
	int fd;
	uint32 i;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, 394240), 0);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 394240, out),
				 CLUSTER_VOTING_DISK_RAW_READ_CLEAN_EOF);
	for (i = 0; i < CLUSTER_VOTING_SLOT_BYTES; i++) {
		shared[i] = (uint8)(i ^ 0x39);
		local0[i] = (uint8)(i ^ 0x6d);
		local127[i] = (uint8)(i ^ 0xc3);
	}

	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(fd, 394240, shared),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(fd, 394752, local0),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(fd, 459776, local127),
				 CLUSTER_VOTING_DISK_IO_OK);
	UT_ASSERT_EQ(fstat(fd, &st), 0);
	UT_ASSERT_EQ(st.st_size, 460288);

	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 394240, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(out, shared, sizeof(out)), 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 394752, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(out, local0, sizeof(out)), 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 459776, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(out, local127, sizeof(out)), 0);

	cluster_voting_disk_close(fd);
	fd = open(path, O_RDONLY, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	memset(out, 0, sizeof(out));
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 459776, out),
				 CLUSTER_VOTING_DISK_RAW_READ_FULL);
	UT_ASSERT_EQ(memcmp(out, local127, sizeof(out)), 0);
	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


UT_TEST(test_io_21_offset_raw_slot_distinguishes_short_and_io_failure)
{
	char *path = make_temp_path("offset_raw_short");
	uint8 slot[CLUSTER_VOTING_SLOT_BYTES];
	int pipefd[2];
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	memset(slot, 0x5a, sizeof(slot));
	UT_ASSERT_EQ(pwrite(fd, slot, 127, 394240), 127);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(fd, 394240, slot),
				 CLUSTER_VOTING_DISK_RAW_READ_SHORT);
	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);

	UT_ASSERT_EQ(pipe(pipefd), 0);
	UT_ASSERT_EQ(cluster_voting_disk_read_raw_slot_at(pipefd[0], 394240, slot),
				 CLUSTER_VOTING_DISK_RAW_READ_IO_FAILED);
	UT_ASSERT_EQ(cluster_voting_disk_write_raw_slot_at(pipefd[1], 394240, slot),
				 CLUSTER_VOTING_DISK_IO_FAILED);
	(void)close(pipefd[0]);
	(void)close(pipefd[1]);
}


UT_TEST(test_io_22_pgrd_authority_bounds_nonlinux_development_file)
{
	char *path = make_temp_path("pgrd_attest");
	int pipefd[2];
	int fd;

	fd = open(path, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
	UT_ASSERT(fd >= 0);
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGRD_FILE_BYTES_MIN), 0);
#ifndef __linux__
	UT_ASSERT(cluster_voting_disk_pgrd_authority_attest(fd));
#else
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(fd));
#endif
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGRD_FILE_BYTES_MIN - 1), 0);
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(fd));
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGRD_FILE_BYTES_MIN + 1), 0);
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(fd));
	UT_ASSERT_EQ(ftruncate(fd, CLUSTER_VOTING_PGRD_FILE_BYTES_MIN), 0);
	cluster_voting_disk_close(fd);
	fd = open(path, O_RDONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(fd));
	cluster_voting_disk_close(fd);
	fd = open(path, O_WRONLY);
	UT_ASSERT(fd >= 0);
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(fd));
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(-1));
	UT_ASSERT_EQ(pipe(pipefd), 0);
	UT_ASSERT(!cluster_voting_disk_pgrd_authority_attest(pipefd[0]));
	(void)close(pipefd[0]);
	(void)close(pipefd[1]);

	cluster_voting_disk_close(fd);
	(void)unlink(path);
	free(path);
}


int
main(void)
{
	UT_PLAN(22);
	UT_RUN(test_io_1_round_trip);
	UT_RUN(test_io_2_crc_mismatch_returns_torn);
	UT_RUN(test_io_3_magic_mismatch_failed);
	UT_RUN(test_io_4_node_id_mismatch_failed);
	UT_RUN(test_io_5_short_read_returns_failed);
	UT_RUN(test_io_7_disk_index_misroute_failed);
	UT_RUN(test_io_6_fd_negative_not_tried);
	UT_RUN(test_io_8_apply_lease_region_round_trip);
	UT_RUN(test_io_9_marker_regions_are_disjoint);
	UT_RUN(test_io_10_raw_tail_state_and_invalid_inputs);
	UT_RUN(test_io_11_raw_tail_read_distinguishes_eof_short_and_full);
	UT_RUN(test_io_12_raw_tail_write_lazily_extends_old_file);
	UT_RUN(test_io_13_raw_tail_syscall_errors_are_io_failed);
	UT_RUN(test_io_14_epoch_ballot_slot_rejects_invalid_inputs);
	UT_RUN(test_io_15_epoch_ballot_slots_round_trip_without_aliasing);
	UT_RUN(test_io_16_epoch_ballot_last_lane_requires_full_sector);
	UT_RUN(test_io_17_format_preallocates_complete_voting_map);
	UT_RUN(test_io_18_epoch_ballot_authority_rejects_fixture_file);
	UT_RUN(test_io_19_offset_raw_slot_rejects_invalid_inputs);
	UT_RUN(test_io_20_pgrd_offsets_round_trip_without_aliasing);
	UT_RUN(test_io_21_offset_raw_slot_distinguishes_short_and_io_failure);
	UT_RUN(test_io_22_pgrd_authority_bounds_nonlinux_development_file);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
