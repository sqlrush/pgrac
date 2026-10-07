/* Actual initial catalog files, source identity and failed publication.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/catversion.h"
#include "catalog/pg_control.h"
#include "cluster/cluster_xid_authority.h"
#include "unit_test.h"
#include "../../backend/cluster/cluster_initdb_catalog_private.h"
UT_DEFINE_GLOBALS();

static char path[MAXPGPATH];
static int base, clogdir, global;
static ControlFileData control;
static ClusterCatalogInitialInput input;
static uint8 clog_bytes[BLCKSZ];
static unsigned io_fault;
static const char *const names[]
	= { "pgrac_oid_authority",	   "pgrac_catalog_authority", "pgrac_xid_authority",
		"pgrac_xid_authority.bak", "pgrac_xid_prehistory",	  "pgrac_xid_prehistory.bak" };

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static ssize_t
test_pread(int fd, void *bytes, size_t length, off_t offset)
{
	ssize_t result;
	if (io_fault == 1) {
		errno = EIO;
		return -1;
	}
	if (io_fault == 2)
		return 0;
	result = pread(fd, bytes, length, offset);
	if (io_fault == 3) {
		int writer = openat(clogdir, "0000", O_WRONLY);
		if (writer < 0 || pwrite(writer, "X", 1, 0) != 1 || close(writer) != 0)
			abort();
		io_fault = 0;
	}
	return result;
}
static int
test_fsync(int fd)
{
	if (io_fault == 4) {
		errno = EIO;
		return -1;
	}
	return fsync(fd);
}
#define pread test_pread
#define fsync test_fsync
#include "../../backend/cluster/cluster_initdb_catalog.c"
#include "../../backend/cluster/cluster_initdb_origin.c"
#undef pread
#undef fsync

static void
put(int directory, const char *name, const void *bytes, Size length)
{
	int fd = openat(directory, name, O_WRONLY | O_CREAT | O_EXCL, 0600);
	UT_ASSERT(fd >= 0 && write(fd, bytes, length) == length && close(fd) == 0);
}
static void
prepare(void)
{
	char temp[] = "/tmp/pgrac-init-catalog-XXXXXX";
	UT_ASSERT(mkdtemp(temp) != NULL);
	strlcpy(path, temp, sizeof(path));
	base = open(path, O_RDONLY | O_DIRECTORY);
	UT_ASSERT(base >= 0 && mkdirat(base, "clog", 0700) == 0 && mkdirat(base, "global", 0700) == 0);
	clogdir = openat(base, "clog", O_RDONLY | O_DIRECTORY);
	global = openat(base, "global", O_RDONLY | O_DIRECTORY);
	UT_ASSERT(clogdir >= 0 && global >= 0);
	memset(&control, 0, sizeof(control));
	memset(&input, 0, sizeof(input));
	memset(clog_bytes, 0x55, sizeof(clog_bytes));
	control.system_identifier = 123456;
	control.pg_control_version = PG_CONTROL_VERSION;
	control.catalog_version_no = CATALOG_VERSION_NO;
	control.blcksz = BLCKSZ;
	control.state = DB_SHUTDOWNED;
	control.checkPoint = control.checkPointCopy.redo = 0x2000028;
	control.checkPointCopy.ThisTimeLineID = control.checkPointCopy.PrevTimeLineID = 1;
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(750);
	control.checkPointCopy.oldestXid = 3;
	control.checkPointCopy.nextOid = 13253;
	control.checkPointCopy.nextMulti = control.checkPointCopy.oldestMulti = 1;
	INIT_CRC32C(control.crc);
	COMP_CRC32C(control.crc, &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(control.crc);
	input.identity.system_identifier = control.system_identifier;
	input.identity.database_incarnation = 1;
	memset(input.identity.storage_uuid, 0x23, 16);
	memset(input.identity.authority_uuid, 0x45, 16);
	input.native_control = &control;
	input.native_control_length = sizeof(control);
	input.native_clog = clog_bytes;
	input.native_clog_length = sizeof(clog_bytes);
	put(clogdir, "0000", clog_bytes, sizeof(clog_bytes));
	io_fault = 0;
}
static void
cleanup(void)
{
	io_fault = 0;
	for (unsigned i = 0; i < lengthof(names); i++)
		(void)unlinkat(global, names[i], 0);
	for (unsigned i = 0; i < 8; i++) {
		char name[8];
		snprintf(name, sizeof(name), "%04X", i);
		(void)unlinkat(clogdir, name, 0);
	}
	(void)unlinkat(clogdir, "old", 0);
	UT_ASSERT(close(clogdir) == 0 && close(global) == 0);
	UT_ASSERT(unlinkat(base, "clog", AT_REMOVEDIR) == 0
			  && unlinkat(base, "global", AT_REMOVEDIR) == 0);
	UT_ASSERT(close(base) == 0 && rmdir(path) == 0);
}
static void
absent(void)
{
	struct stat st;
	for (unsigned i = 0; i < lengthof(names); i++)
		UT_ASSERT(fstatat(global, names[i], &st, AT_SYMLINK_NOFOLLOW) != 0 && errno == ENOENT);
}

UT_TEST(original_files_are_exclusive_and_exact)
{
	uint8 *bytes = NULL;
	Size length = 0;
	prepare();
	UT_ASSERT(cluster_initdb_catalog_read_clog(clogdir, 750, &bytes, &length));
	UT_ASSERT(length == sizeof(clog_bytes) && memcmp(bytes, clog_bytes, length) == 0);
	free(bytes);
	UT_ASSERT(cluster_initdb_catalog_create(global, &input));
	for (unsigned i = 0; i < lengthof(names); i++) {
		unsigned kind = i < 2 ? i : i < 4 ? 2 : 3;
		Size required = cluster_catalog_initial_image_size(kind, &input);
		uint8 *expected = calloc(1, required);
		struct stat st;
		UT_ASSERT(expected != NULL
				  && cluster_catalog_initial_image(kind, &input, expected, required));
		UT_ASSERT(fstatat(global, names[i], &st, AT_SYMLINK_NOFOLLOW) == 0);
		UT_ASSERT(cluster_initdb_object_recheck(global, names[i], expected, required, &st));
		free(expected);
	}
	UT_ASSERT(!cluster_initdb_catalog_create(global, &input));
	cleanup();
}
UT_TEST(invalid_native_input_creates_nothing)
{
	prepare();
	control.crc++;
	UT_ASSERT(!cluster_initdb_catalog_create(global, &input));
	absent();
	cleanup();
	prepare();
	clog_bytes[1] = 0xff;
	UT_ASSERT(!cluster_initdb_catalog_create(global, &input));
	absent();
	cleanup();
}
UT_TEST(any_occupied_target_prevents_all_publication)
{
	prepare();
	put(global, names[5], "keep", 4);
	UT_ASSERT(!cluster_initdb_catalog_create(global, &input));
	for (unsigned i = 0; i < 5; i++) {
		struct stat st;
		UT_ASSERT(fstatat(global, names[i], &st, 0) != 0 && errno == ENOENT);
	}
	{
		int fd = openat(global, names[5], O_RDONLY);
		char bytes[5] = { 0 };
		UT_ASSERT(fd >= 0 && read(fd, bytes, 5) == 4 && memcmp(bytes, "keep", 4) == 0
				  && close(fd) == 0);
	}
	cleanup();
}
UT_TEST(source_alias_or_unsafe_file_is_refused)
{
	for (unsigned fault = 0; fault < 4; fault++) {
		uint8 *bytes = NULL;
		Size length = 99;
		prepare();
		UT_ASSERT(renameat(clogdir, "0000", clogdir, "old") == 0);
		if (fault == 0)
			UT_ASSERT(symlinkat("old", clogdir, "0000") == 0);
		if (fault == 1)
			UT_ASSERT(linkat(clogdir, "old", clogdir, "0000", 0) == 0);
		if (fault == 2)
			UT_ASSERT(mkfifoat(clogdir, "0000", 0600) == 0);
		if (fault == 3) {
			UT_ASSERT(renameat(clogdir, "old", clogdir, "0000") == 0);
			UT_ASSERT(fchmodat(clogdir, "0000", 0666, 0) == 0);
		}
		UT_ASSERT(!cluster_initdb_catalog_read_clog(clogdir, 750, &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
		absent();
		cleanup();
	}
}
UT_TEST(read_failure_and_changed_source_clear_result)
{
	for (unsigned fault = 1; fault <= 3; fault++) {
		uint8 *bytes = NULL;
		Size length = 99;
		prepare();
		io_fault = fault;
		UT_ASSERT(!cluster_initdb_catalog_read_clog(clogdir, 750, &bytes, &length));
		UT_ASSERT(bytes == NULL && length == 0);
		absent();
		cleanup();
	}
}
UT_TEST(exact_page_range_is_required)
{
	uint8 *bytes = NULL;
	Size length = 0;
	prepare();
	UT_ASSERT(!cluster_initdb_catalog_read_clog(clogdir, 2, &bytes, &length));
	UT_ASSERT(!cluster_initdb_catalog_read_clog(clogdir, CLUSTER_XID_PREHISTORY_MAX_XID + 1, &bytes,
												&length));
	UT_ASSERT(!cluster_initdb_catalog_read_clog(clogdir, BLCKSZ * 4 + 1, &bytes, &length));
	UT_ASSERT(bytes == NULL && length == 0);
	cleanup();
}
UT_TEST(durable_write_failure_does_not_claim_success)
{
	prepare();
	io_fault = 4;
	UT_ASSERT(!cluster_initdb_catalog_create(global, &input));
	io_fault = 0;
	UT_ASSERT(!cluster_initdb_catalog_create(global, &input));
	cleanup();
}
UT_TEST(maximum_native_range_crosses_real_segments)
{
	uint8 *bytes = NULL;
	Size length = 0;
	uint8 *segment = calloc(32, BLCKSZ);
	prepare();
	UT_ASSERT(segment != NULL && unlinkat(clogdir, "0000", 0) == 0);
	for (unsigned i = 0; i < 8; i++) {
		char name[8];
		snprintf(name, sizeof(name), "%04X", i);
		memset(segment, 0x55, 32 * BLCKSZ);
		segment[0] = (i % 2) ? 0xaa : 0x55;
		put(clogdir, name, segment, 32 * BLCKSZ);
	}
	UT_ASSERT(
		cluster_initdb_catalog_read_clog(clogdir, CLUSTER_XID_PREHISTORY_MAX_XID, &bytes, &length));
	UT_ASSERT(length == CLUSTER_XID_PREHISTORY_MAX_XID / 4);
	for (unsigned i = 0; i < 8; i++)
		UT_ASSERT(bytes[i * 32 * BLCKSZ] == ((i % 2) ? 0xaa : 0x55));
	/* Exercise the actual bounded writer with the largest prehistory object. */
	control.checkPointCopy.nextXid = FullTransactionIdFromU64(CLUSTER_XID_PREHISTORY_MAX_XID);
	INIT_CRC32C(control.crc);
	COMP_CRC32C(control.crc, &control, offsetof(ControlFileData, crc));
	FIN_CRC32C(control.crc);
	input.native_clog = bytes;
	input.native_clog_length = length;
	UT_ASSERT(cluster_initdb_catalog_create(global, &input));
	{
		struct stat st;
		UT_ASSERT(fstatat(global, names[4], &st, 0) == 0);
		UT_ASSERT(st.st_size == length + sizeof(ClusterXidPrehistoryHeader));
	}
	free(bytes);
	free(segment);
	cleanup();
}
int
main(void)
{
	UT_PLAN(8);
	UT_RUN(original_files_are_exclusive_and_exact);
	UT_RUN(invalid_native_input_creates_nothing);
	UT_RUN(any_occupied_target_prevents_all_publication);
	UT_RUN(source_alias_or_unsafe_file_is_refused);
	UT_RUN(read_failure_and_changed_source_clear_result);
	UT_RUN(exact_page_range_is_required);
	UT_RUN(durable_write_failure_does_not_claim_success);
	UT_RUN(maximum_native_range_crosses_real_segments);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
