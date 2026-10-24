/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_arb.c
 *    Exercise crash arbitration against real relmap authority files.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_arb.c
 *
 * NOTES
 *    This is a pgrac-original test. Native status, foreign outcomes and
 *    backend runtime boundaries are fixtures; arbitration, transaction
 *    status interpretation and authority file operations are unchanged.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../../backend/access/transam/transam.c"
#include "../../backend/cluster/cluster_relmap_authority.c"
#include "../../backend/cluster/cluster_relmap_arb.c"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

char *cluster_shared_data_dir;
bool enableFsync = true;
bool cluster_enabled = true;
bool cluster_shared_catalog = true;
int cluster_node_id = 3;
TransactionId TransactionXmin = FirstNormalTransactionId;

static char test_root[MAXPGPATH];
static char primary[MAXPGPATH], backup[MAXPGPATH];
static char original_primary[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
static char original_backup[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
static TransactionId owner_xid = 100, next_xid = 1000, parent_xid;
static XidStatus native_status, parent_status;
static ClusterRemoteXactOutcome remote_status;
static unsigned int native_reads, parent_reads, remote_reads, writes, invalidations, acks;
static int remote_origin;
static TransactionId remote_xid;
static ClusterConf topology;
ClusterConf *ClusterConfShmem = &topology;

/* Failures in real authority I/O must fail the test, not become success. */
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "# Assert failed: %s at %s:%d\n", condition, file, line);
	abort();
}

bool
errstart(int elevel, const char *domain pg_attribute_unused())
{
	if (elevel >= ERROR)
		abort();
	return false;
}

bool
errstart_cold(int elevel, const char *domain)
{
	return errstart(elevel, domain);
}
void
errfinish(const char *file pg_attribute_unused(), int line pg_attribute_unused(),
		  const char *func pg_attribute_unused())
{}
int
errcode(int code pg_attribute_unused())
{
	return 0;
}
int
errcode_for_file_access(void)
{
	return 0;
}
int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}
char *
psprintf(const char *fmt pg_attribute_unused(), ...)
{
	abort();
}

int
OpenTransientFile(const char *path, int flags)
{
	return open(path, flags, 0600);
}
int
CloseTransientFile(int fd)
{
	return close(fd);
}
int
pg_fsync(int fd)
{
	return fsync(fd);
}
int
durable_rename(const char *old, const char *new, int elevel pg_attribute_unused())
{
	writes++;
	return rename(old, new);
}

FullTransactionId
ReadNextFullTransactionId(void)
{
	return FullTransactionIdFromU64(next_xid);
}
XidStatus
TransactionIdGetStatus(TransactionId xid, XLogRecPtr *lsn)
{
	native_reads++;
	*lsn = InvalidXLogRecPtr;
	if (xid == owner_xid)
		return native_status;
	if (TransactionIdIsValid(parent_xid) && xid == parent_xid)
		return parent_status;
	abort();
}

TransactionId
SubTransGetParent(TransactionId xid)
{
	UT_ASSERT_EQ(xid, owner_xid);
	parent_reads++;
	return parent_xid;
}

ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked(int origin, TransactionId xid, SCN *scn)
{
	remote_reads++;
	remote_origin = origin;
	remote_xid = xid;
	*scn = 0;
	return remote_status;
}

/* Check the actual committed image at the invalidation boundary. */
void
SendSharedInvalidMessages(const SharedInvalidationMessage *msgs, int n)
{
	ClusterRelmapAuthorityHeader hdr;
	char image[CLUSTER_RELMAP_IMAGE_MAX];
	char expected[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 len = 0;

	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.id, SHAREDINVALRELMAP_ID);
	UT_ASSERT_EQ(msgs[0].rm.dbId, InvalidOid);
	UT_ASSERT(cluster_relmap_authority_read_header(true, InvalidOid, &hdr));
	UT_ASSERT_EQ(hdr.pending_generation, 0);
	UT_ASSERT_EQ(hdr.committed_generation, 2);
	UT_ASSERT(cluster_relmap_authority_read_committed(true, InvalidOid, image, &len));
	UT_ASSERT_EQ(len, CLUSTER_RELMAP_IMAGE_MAX);
	memset(expected, 0x22, sizeof(expected));
	UT_ASSERT_EQ(memcmp(image, expected, sizeof(image)), 0);
	invalidations++;
}

ClusterSinvalAckResult
cluster_sinval_enqueue_and_wait_ack(const SharedInvalidationMessage *msgs, int n)
{
	UT_ASSERT_EQ(invalidations, 1);
	UT_ASSERT_EQ(n, 1);
	UT_ASSERT_EQ(msgs[0].rm.dbId, InvalidOid);
	acks++;
	return CLUSTER_SINVAL_ACK_DONE;
}

static void
read_file(const char *path, char *image)
{
	int fd = open(path, O_RDONLY);

	UT_ASSERT(fd >= 0);
	if (fd < 0)
		abort();
	UT_ASSERT_EQ(read(fd, image, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE),
				 CLUSTER_RELMAP_AUTHORITY_FILE_SIZE);
	UT_ASSERT_EQ(close(fd), 0);
}

static void
snapshot(void)
{
	read_file(primary, original_primary);
	read_file(backup, original_backup);
}

/* Each scenario begins with a different committed and pending image. */
static void
stage_pending(int node, TransactionId xid)
{
	ClusterRelmapOwner owner = { node, xid, 9, 0x10000 };
	char image[CLUSTER_RELMAP_IMAGE_MAX];

	unlink(primary);
	unlink(backup);
	memset(image, 0x11, sizeof(image));
	cluster_relmap_authority_write_pending(true, InvalidOid, image, sizeof(image), 1, &owner);
	cluster_relmap_authority_publish(true, InvalidOid, 1);
	memset(image, 0x22, sizeof(image));
	cluster_relmap_authority_write_pending(true, InvalidOid, image, sizeof(image), 2, &owner);
	owner_xid = xid;
	next_xid = 1000;
	parent_xid = InvalidTransactionId;
	TransactionXmin = FirstNormalTransactionId;
	/* The real transam cache cannot carry a terminal result between scenarios. */
	cachedFetchXid = InvalidTransactionId;
	native_status = TRANSACTION_STATUS_IN_PROGRESS;
	parent_status = TRANSACTION_STATUS_COMMITTED;
	remote_status = CLUSTER_REMOTE_XACT_INDOUBT;
	native_reads = parent_reads = remote_reads = writes = invalidations = acks = 0;
	topology.node_count = 1;
	snapshot();
}

static void
check_committed(unsigned char marker, uint64 generation)
{
	ClusterRelmapAuthorityHeader hdr;
	char image[CLUSTER_RELMAP_IMAGE_MAX];
	char expected[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 len = 0;

	UT_ASSERT(cluster_relmap_authority_read_header(true, InvalidOid, &hdr));
	UT_ASSERT_EQ(hdr.committed_generation, generation);
	UT_ASSERT(cluster_relmap_authority_read_committed(true, InvalidOid, image, &len));
	UT_ASSERT_EQ(len, sizeof(image));
	memset(expected, marker, sizeof(expected));
	UT_ASSERT_EQ(memcmp(image, expected, sizeof(image)), 0);
}

static void
check_unchanged(void)
{
	char image[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];

	read_file(primary, image);
	UT_ASSERT_EQ(memcmp(image, original_primary, sizeof(image)), 0);
	read_file(backup, image);
	UT_ASSERT_EQ(memcmp(image, original_backup, sizeof(image)), 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(invalidations, 0);
	UT_ASSERT_EQ(acks, 0);
	check_committed(0x11, 1);
}

UT_TEST(test_in_progress_retains_pending)
{
	stage_pending(cluster_node_id, 100);
	arbitrate_one(true, InvalidOid);
	check_unchanged();
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(remote_reads, 0);
}

UT_TEST(test_subcommitted_missing_parent_retains_pending)
{
	stage_pending(cluster_node_id, 100);
	native_status = TRANSACTION_STATUS_SUB_COMMITTED;
	arbitrate_one(true, InvalidOid);
	check_unchanged();
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(parent_reads, 0);
}

UT_TEST(test_subcommitted_committed_parent_cannot_publish)
{
	stage_pending(cluster_node_id, 100);
	native_status = TRANSACTION_STATUS_SUB_COMMITTED;
	parent_xid = 90;
	arbitrate_one(true, InvalidOid);
	check_unchanged();
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(parent_reads, 0);
}

UT_TEST(test_subcommitted_below_xmin_retains_pending)
{
	stage_pending(cluster_node_id, 100);
	native_status = TRANSACTION_STATUS_SUB_COMMITTED;
	TransactionXmin = 101;
	arbitrate_one(true, InvalidOid);
	check_unchanged();
	UT_ASSERT_EQ(parent_reads, 0);
}

UT_TEST(test_unrecognized_status_retains_pending)
{
	stage_pending(cluster_node_id, 100);
	native_status = 0xff;
	arbitrate_one(true, InvalidOid);
	check_unchanged();
}

UT_TEST(test_explicit_commit_publishes_before_invalidation)
{
	stage_pending(cluster_node_id, 100);
	native_status = TRANSACTION_STATUS_COMMITTED;
	topology.node_count = 2;
	arbitrate_one(true, InvalidOid);
	check_committed(0x22, 2);
	UT_ASSERT(writes > 0);
	UT_ASSERT_EQ(invalidations, 1);
	UT_ASSERT_EQ(acks, 1);
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(remote_reads, 0);
}

UT_TEST(test_explicit_abort_discards_without_invalidation)
{
	ClusterRelmapAuthorityHeader hdr;

	stage_pending(cluster_node_id, 100);
	native_status = TRANSACTION_STATUS_ABORTED;
	arbitrate_one(true, InvalidOid);
	check_committed(0x11, 1);
	UT_ASSERT(cluster_relmap_authority_read_header(true, InvalidOid, &hdr));
	UT_ASSERT_EQ(hdr.pending_generation, 0);
	UT_ASSERT(writes > 0);
	UT_ASSERT_EQ(invalidations, 0);
	UT_ASSERT_EQ(native_reads, 1);
	UT_ASSERT_EQ(remote_reads, 0);
}

UT_TEST(test_invalid_or_unissued_owner_is_not_looked_up)
{
	TransactionId candidates[]
		= { InvalidTransactionId, BootstrapTransactionId, FrozenTransactionId, 1000, 1001 };

	for (size_t i = 0; i < lengthof(candidates); i++) {
		stage_pending(cluster_node_id, candidates[i]);
		native_status = TRANSACTION_STATUS_COMMITTED;
		arbitrate_one(true, InvalidOid);
		check_unchanged();
		UT_ASSERT_EQ(native_reads, 0);
	}
}

UT_TEST(test_clean_authority_does_not_look_up_status)
{
	stage_pending(cluster_node_id, 100);
	cluster_relmap_authority_discard_pending(true, InvalidOid, 2);
	writes = 0;
	snapshot();
	arbitrate_one(true, InvalidOid);
	check_unchanged();
	UT_ASSERT_EQ(native_reads, 0);
	UT_ASSERT_EQ(remote_reads, 0);
}

UT_TEST(test_unreadable_authority_does_not_look_up_status)
{
	stage_pending(cluster_node_id, 100);
	UT_ASSERT_EQ(truncate(primary, 1), 0);
	UT_ASSERT_EQ(truncate(backup, 1), 0);
	arbitrate_one(true, InvalidOid);
	UT_ASSERT_EQ(native_reads, 0);
	UT_ASSERT_EQ(remote_reads, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(invalidations, 0);
	unlink(primary);
	unlink(backup);
	arbitrate_one(true, InvalidOid);
	UT_ASSERT_EQ(native_reads, 0);
	UT_ASSERT_EQ(remote_reads, 0);
	UT_ASSERT_EQ(writes, 0);
}

UT_TEST(test_foreign_unknown_uses_original_origin_and_retains_pending)
{
	stage_pending(7, 100);
	native_status = TRANSACTION_STATUS_COMMITTED;
	arbitrate_one(true, InvalidOid);
	check_unchanged();
	UT_ASSERT_EQ(native_reads, 0);
	UT_ASSERT_EQ(remote_reads, 1);
	UT_ASSERT_EQ(remote_origin, 7);
	UT_ASSERT_EQ(remote_xid, 100);
}

UT_TEST(test_foreign_terminal_paths_do_not_read_local_clog)
{
	ClusterRelmapAuthorityHeader hdr;

	stage_pending(7, 100);
	remote_status = CLUSTER_REMOTE_XACT_COMMITTED;
	arbitrate_one(true, InvalidOid);
	check_committed(0x22, 2);
	UT_ASSERT_EQ(native_reads, 0);
	UT_ASSERT_EQ(remote_reads, 1);
	UT_ASSERT_EQ(invalidations, 1);
	stage_pending(7, 100);
	remote_status = CLUSTER_REMOTE_XACT_ABORTED;
	arbitrate_one(true, InvalidOid);
	check_committed(0x11, 1);
	UT_ASSERT(cluster_relmap_authority_read_header(true, InvalidOid, &hdr));
	UT_ASSERT_EQ(hdr.pending_generation, 0);
	UT_ASSERT_EQ(native_reads, 0);
	UT_ASSERT_EQ(remote_reads, 1);
	UT_ASSERT_EQ(invalidations, 0);
}

int
main(void)
{
	char path[MAXPGPATH];

	snprintf(test_root, sizeof(test_root), "/tmp/pgrac_relmap_arb_XXXXXX");
	if (mkdtemp(test_root) == NULL)
		return 1;
	cluster_shared_data_dir = test_root;
	snprintf(path, sizeof(path), "%s/global", test_root);
	if (mkdir(path, 0700) != 0)
		return 1;
	snprintf(primary, sizeof(primary), "%s/pgrac_relmap_authority", path);
	snprintf(backup, sizeof(backup), "%s.bak", primary);
	UT_PLAN(12);
	UT_RUN(test_in_progress_retains_pending);
	UT_RUN(test_subcommitted_missing_parent_retains_pending);
	UT_RUN(test_subcommitted_committed_parent_cannot_publish);
	UT_RUN(test_subcommitted_below_xmin_retains_pending);
	UT_RUN(test_unrecognized_status_retains_pending);
	UT_RUN(test_explicit_commit_publishes_before_invalidation);
	UT_RUN(test_explicit_abort_discards_without_invalidation);
	UT_RUN(test_invalid_or_unissued_owner_is_not_looked_up);
	UT_RUN(test_clean_authority_does_not_look_up_status);
	UT_RUN(test_unreadable_authority_does_not_look_up_status);
	UT_RUN(test_foreign_unknown_uses_original_origin_and_retains_pending);
	UT_RUN(test_foreign_terminal_paths_do_not_read_local_clog);
	unlink(primary);
	unlink(backup);
	rmdir(path);
	rmdir(test_root);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
