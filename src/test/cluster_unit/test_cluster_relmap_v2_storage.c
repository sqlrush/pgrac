/*-------------------------------------------------------------------------
 *
 * test_cluster_relmap_v2_storage.c
 *    Real filesystem tests of the production relmap v2 storage entry points.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/test/cluster_unit/test_cluster_relmap_v2_storage.c
 *
 * NOTES
 *    Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_relmap_authority.h"
#include "cluster/cluster_relmap_lock.h"
#include "cluster/cluster_relmap_pending.h"
#include "cluster/cluster_relmap_retention_restore.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/lwlock.h"

#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"

UT_DEFINE_GLOBALS();

char *cluster_shared_data_dir;
bool enableFsync = true;

static char root[MAXPGPATH];
static char directory[MAXPGPATH];
/* Selected namespace pin at the serialized stage write boundary. */
static unsigned stage_pin_index;
static unsigned int writes, renames, file_syncs, dir_syncs, reads;
static unsigned int fail_write, fail_rename, fail_file_sync, fail_dir_sync;
static bool corrupt_after_rename, replace_during_read, replace_after_rename;
static unsigned int fault_rename;
static int fault_copy;
static bool fail_directory_close;
static LWLock retention_lock;
static LWLock *runtime_test_lock, *held_retention_lock;
static bool retention_held, observing_stage, relmap_x_held;
static bool mapping_held;
static bool drop_relmap_x_on_read;
static bool cancel_on_read, invalidate_on_read, cancel_on_write;
static unsigned int io_under_retention_lock, writes_without_pin;
static unsigned int retention_acquires, retention_releases;
static ClusterRelmapRetentionDirectory writer_retention;
static ClusterRelmapRetentionDirectory *stage_directory = &writer_retention;
static LWLock *stage_lock = &retention_lock;
static void (*post_sync_hook)(bool is_directory);
static ClusterRelmapRetentionKey writer_key;
static ClusterRelmapRetentionResult cancelled;
static const char *const names[] = {
	"pgrac_relmap_authority", "pgrac_relmap_authority.bak",
	"pgrac_relmap_authority.tmp", "pgrac_relmap_authority.bak.tmp", "replacement"
};
static const ClusterRelmapV2Namespace identity = { 12345, 67890, InvalidOid, true };
static const ClusterRelmapV2Owner owner = { 3, UINT64CONST(0x200000123), 0, 101, 909, 202 };

/* Serialized backend lock boundary, not a concurrent LWLock implementation. */
bool
LWLockAcquire(LWLock *lock, LWLockMode mode)
{
	if ((lock != &retention_lock && lock != runtime_test_lock)
		|| mode != LW_EXCLUSIVE || retention_held)
		abort();
	retention_held = true;
	held_retention_lock = lock;
	retention_acquires++;
	return true;
}

void
LWLockRelease(LWLock *lock)
{
	if (lock != held_retention_lock || !retention_held)
		abort();
	retention_held = false;
	held_retention_lock = NULL;
	retention_releases++;
}

bool
LWLockHeldByMe(LWLock *lock)
{
	return (lock == held_retention_lock && retention_held)
		|| (lock == RelationMappingLock && mapping_held);
}

bool
cluster_relmap_authority_x_held(void)
{
	return relmap_x_held;
}

static void
observe_io(void)
{
	if (retention_held)
		io_under_retention_lock++;
}

/* Only backend facilities are supplied here.  Every successful I/O is real. */
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	fprintf(stderr, "unexpected Assert: %s at %s:%d\n", condition, file, line);
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
OpenTransientFile(const char *name, int flags)
{
	return open(name, flags, 0600);
}

int
CloseTransientFile(int fd)
{
	return close(fd);
}

int
durable_rename(const char *old, const char *new, int elevel pg_attribute_unused())
{
	/* The v1 implementation is linked but is never called by these tests. */
	abort();
}

int
pg_fsync(int fd)
{
	struct stat st;
	unsigned int call;
	unsigned int fail;
	int rc;

	observe_io();
	if (fstat(fd, &st) != 0)
		abort();
	if (S_ISDIR(st.st_mode)) {
		call = ++dir_syncs;
		fail = fail_dir_sync;
	} else {
		call = ++file_syncs;
		fail = fail_file_sync;
	}
	if (fail != 0 && call == fail) {
		errno = EIO;
		return -1;
	}
	rc = fsync(fd);
	if (rc == 0 && post_sync_hook != NULL)
		post_sync_hook(S_ISDIR(st.st_mode));
	return rc;
}

/* Same product source compiled with syscall names redirected, no body copy. */
ssize_t relmap_test_write(int fd, const void *bytes, size_t len);
ssize_t relmap_test_read(int fd, void *bytes, size_t len);
int relmap_test_renameat(int olddir, const char *old, int newdir, const char *new);
int relmap_test_close(int fd);

int
relmap_test_close(int fd)
{
	struct stat st;
	bool directory;
	int rc;

	observe_io();
	if (fstat(fd, &st) != 0)
		abort();
	directory = S_ISDIR(st.st_mode);
	rc = close(fd);
	if (directory && fail_directory_close) {
		fail_directory_close = false;
		errno = EIO;
		return -1;
	}
	return rc;
}

ssize_t
relmap_test_write(int fd, const void *bytes, size_t len)
{
	observe_io();
	if (observing_stage) {
		const ClusterRelmapRetentionPin *pin = &stage_directory->pins[stage_pin_index];

		if (!pin->occupied || !pin->may_be_staged || pin->wal_end != owner.wal_end)
			writes_without_pin++;
		if (cancel_on_write) {
			cancel_on_write = false;
			LWLockAcquire(stage_lock, LW_EXCLUSIVE);
			cancelled = cluster_relmap_retention_cancel_prepare(stage_directory, &writer_key);
			LWLockRelease(stage_lock);
		}
	}
	if (++writes == fail_write) {
		errno = ENOSPC;
		return -1;
	}
	return write(fd, bytes, len);
}

static void
replace_primary(void)
{
	char path[MAXPGPATH];
	char replacement[MAXPGPATH];
	unsigned char bytes[2176];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[0]);
	snprintf(replacement, sizeof(replacement), "%s/replacement", directory);
	fd = open(path, O_RDONLY);
	if (fd < 0 || read(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd) != 0)
		abort();
	fd = open(replacement, O_WRONLY | O_CREAT | O_EXCL, 0600);
	if (fd < 0 || write(fd, bytes, sizeof(bytes)) != sizeof(bytes) || close(fd) != 0
		|| rename(replacement, path) != 0)
		abort();
}

ssize_t
relmap_test_read(int fd, void *bytes, size_t len)
{
	ssize_t n = read(fd, bytes, len);

	observe_io();
	if (drop_relmap_x_on_read) {
		drop_relmap_x_on_read = false;
		relmap_x_held = false;
	}
	if (observing_stage && (cancel_on_read || invalidate_on_read)) {
		LWLockAcquire(stage_lock, LW_EXCLUSIVE);
		if (cancel_on_read)
			cancelled = cluster_relmap_retention_cancel_prepare(stage_directory, &writer_key);
		if (invalidate_on_read)
			cluster_relmap_retention_invalidate(stage_directory);
		LWLockRelease(stage_lock);
		cancel_on_read = invalidate_on_read = false;
	}
	reads++;
	if (replace_during_read && reads == 1)
		replace_primary();
	return n;
}

int
relmap_test_renameat(int olddir, const char *old, int newdir, const char *new)
{
	int rc;

	observe_io();
	if (++renames == fail_rename) {
		errno = EIO;
		return -1;
	}
	rc = renameat(olddir, old, newdir, new);
	if (rc == 0 && renames == fault_rename && replace_after_rename)
		replace_primary();
	if (rc == 0 && renames == fault_rename && corrupt_after_rename) {
		/* Also exercise the final recheck of primary after backup installation. */
		int fd = openat(newdir, names[fault_copy], O_WRONLY);
		unsigned char bad = 0xff;

		if (fd < 0 || pwrite(fd, &bad, 1, 128) != 1 || close(fd) != 0)
			abort();
	}
	return rc;
}

static void
cleanup(void)
{
	char path[MAXPGPATH];
	const char *dirs[] = { "global", "base/42" };

	if (root[0] == '\0')
		return;
	for (size_t d = 0; d < lengthof(dirs); d++) {
		for (size_t n = 0; n < lengthof(names); n++) {
			snprintf(path, sizeof(path), "%s/%s/%s", root, dirs[d], names[n]);
			unlink(path);
		}
		snprintf(path, sizeof(path), "%s/%s", root, dirs[d]);
		rmdir(path);
	}
	snprintf(path, sizeof(path), "%s/base", root);
	rmdir(path);
	rmdir(root);
	root[0] = '\0';
}

static RelMapFile
map_image(RelFileNumber number)
{
	RelMapFile map;

	memset(&map, 0, sizeof(map));
	map.magic = RELMAPPER_FILEMAGIC;
	map.num_mappings = 1;
	map.mappings[0].mapoid = 1259;
	map.mappings[0].mapfilenumber = number;
	INIT_CRC32C(map.crc);
	COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
	FIN_CRC32C(map.crc);
	return map;
}

static void
write_copy(int copy, const unsigned char *bytes, size_t len)
{
	char path[MAXPGPATH];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[copy]);
	fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
	if (fd < 0 || write(fd, bytes, len) != len || fsync(fd) != 0 || close(fd) != 0)
		abort();
}

static void
read_copy(int copy, unsigned char bytes[2176])
{
	char path[MAXPGPATH];
	struct stat st;
	int fd;

	snprintf(path, sizeof(path), "%s/%s", directory, names[copy]);
	fd = open(path, O_RDONLY);
	if (fd < 0 || fstat(fd, &st) != 0 || st.st_size != 2176
		|| read(fd, bytes, 2176) != 2176 || close(fd) != 0)
		abort();
}

static void
reset_io(void)
{
	writes = renames = file_syncs = dir_syncs = reads = 0;
	fail_write = fail_rename = fail_file_sync = fail_dir_sync = 0;
	corrupt_after_rename = replace_during_read = replace_after_rename = false;
	fault_rename = 1;
	fault_copy = 0;
	fail_directory_close = false;
	retention_held = observing_stage = relmap_x_held = false;
	stage_pin_index = 0;
	held_retention_lock = NULL;
	mapping_held = false;
	post_sync_hook = NULL;
	drop_relmap_x_on_read = false;
	cancel_on_read = invalidate_on_read = cancel_on_write = false;
	io_under_retention_lock = writes_without_pin = 0;
	retention_acquires = retention_releases = 0;
	cancelled = CLUSTER_RELMAP_RETENTION_UNAVAILABLE;
}

static void
setup(const ClusterRelmapV2Namespace *ns)
{
	ClusterRelmapV2Image image;
	unsigned char bytes[2176];
	char path[MAXPGPATH];

	cleanup();
	stage_directory = &writer_retention;
	stage_lock = &retention_lock;
	strcpy(root, "/tmp/pgrac_relmap_v2_XXXXXX");
	if (mkdtemp(root) == NULL)
		abort();
	snprintf(path, sizeof(path), "%s/global", root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(path, sizeof(path), "%s/base", root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(path, sizeof(path), "%s/base/42", root);
	if (mkdir(path, 0700) != 0)
		abort();
	snprintf(directory, sizeof(directory), "%s/%s", root, ns->shared ? "global" : "base/42");
	cluster_shared_data_dir = root;
	enableFsync = true;
	reset_io();
	memset(&image, 0, sizeof(image));
	image.identity = *ns;
	image.committed_generation = 7;
	image.committed = map_image(40000);
	if (cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)) != CLUSTER_RELMAP_V2_VALID)
		abort();
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	memset(&writer_retention, 0, sizeof(writer_retention));
	memset(&writer_key, 0, sizeof(writer_key));
	writer_key.identity = *ns;
	writer_key.generation = 8;
	writer_key.origin = owner.node;
	writer_key.top_full_xid = owner.top_full_xid;
	writer_key.membership_epoch = owner.membership_epoch;
	writer_key.admitted_incarnation = owner.admitted_incarnation;
	writer_key.boot_incarnation = owner.boot_incarnation;
	if (cluster_relmap_retention_build_start(&writer_retention, ns->system_identifier,
		ns->database_incarnation, owner.node, 1, owner.top_full_xid - 16,
		owner.top_full_xid + 16) != CLUSTER_RELMAP_RETENTION_OK
		|| cluster_relmap_retention_build_finish(&writer_retention) != CLUSTER_RELMAP_RETENTION_OK)
		abort();
	LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
	if (cluster_relmap_retention_begin(&writer_retention, &writer_key)
		!= CLUSTER_RELMAP_RETENTION_OK)
		abort();
	LWLockRelease(&retention_lock);
	reset_io();
}

static bool
stage_request(const ClusterRelmapV2Namespace *ns, uint64 generation,
	const ClusterRelmapV2Owner *request_owner)
{
	RelMapFile map = map_image(40001);
	bool result;

	relmap_x_held = true;
	observing_stage = true;
	result = cluster_relmap_authority_v2_write_pending(ns, generation, request_owner, &map,
		&writer_retention, &retention_lock);
	observing_stage = false;
	relmap_x_held = false;
	return result;
}

static bool
stage(const ClusterRelmapV2Namespace *ns)
{
	return stage_request(ns, 8, &owner);
}

static bool
read_committed_under_x(const ClusterRelmapV2Namespace *ns, RelMapFile *map,
		uint64 *generation)
{
	bool was_held = relmap_x_held;
	bool result;

	relmap_x_held = true;
	result = cluster_relmap_authority_v2_read_committed(ns, map, generation);
	relmap_x_held = was_held;
	return result;
}

UT_TEST(test_stage_both_copies_before_success)
{
	ClusterRelmapV2View view;
	ClusterRelmapV2Image disk;
	unsigned char primary[2176];
	unsigned char backup[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	read_copy(0, primary);
	read_copy(1, backup);
	UT_ASSERT(memcmp(primary, backup, sizeof(primary)) == 0);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(backup, sizeof(backup), &identity, &disk),
				 CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT_EQ(disk.pending_generation, 8);
	UT_ASSERT_EQ(disk.pending_owner.top_full_xid, UINT64CONST(0x200000123));
	UT_ASSERT_EQ(disk.pending_owner.boot_incarnation, 202);
	UT_ASSERT_EQ(disk.pending_owner.admitted_incarnation, 101);
	UT_ASSERT_EQ(disk.pending_owner.membership_epoch, 0);
	UT_ASSERT_EQ(disk.pending_owner.wal_end, 909);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT_EQ(view.committed_generation, 7);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT_EQ(view.pending.mappings[0].mapfilenumber, 40001);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
}

UT_TEST(test_committed_reader_hides_pending_and_rejects_degraded_pair)
{
	ClusterRelmapV2View view;
	RelMapFile map;
	RelMapFile zero;
	unsigned char bytes[2176];
	uint64 generation;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	memset(&map, 0xa5, sizeof(map));
	generation = UINT64CONST(99);
	UT_ASSERT(read_committed_under_x(&identity, &map, &generation));
	UT_ASSERT_EQ(generation, UINT64CONST(7));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)) == 0, false);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.pending_complete && view.has_pending);
	UT_ASSERT_EQ(view.pending.mappings[0].mapfilenumber, 40001);

	read_copy(0, bytes);
	bytes[128] ^= 1;
	write_copy(0, bytes, sizeof(bytes));
	memset(&map, 0xa5, sizeof(map));
	generation = UINT64CONST(99);
	UT_ASSERT(!read_committed_under_x(&identity, &map, &generation));
	UT_ASSERT_EQ(generation, UINT64CONST(0));
	UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)) == 0, true);
}

UT_TEST(test_committed_reader_requires_namespace_x_lock)
{
	RelMapFile map;
	RelMapFile zero;
	uint64 generation;
	unsigned int reads_before;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	reads_before = reads;
	memset(&map, 0xa5, sizeof(map));
	memset(&zero, 0, sizeof(zero));
	generation = UINT64CONST(99);
	relmap_x_held = false;
	UT_ASSERT(!cluster_relmap_authority_v2_read_committed(&identity, &map, &generation));
	UT_ASSERT_EQ(generation, UINT64CONST(0));
	UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)) == 0, true);
	UT_ASSERT_EQ(reads, reads_before);

	relmap_x_held = true;
	UT_ASSERT(cluster_relmap_authority_v2_read_committed(&identity, &map, &generation));
	relmap_x_held = false;
	UT_ASSERT_EQ(generation, UINT64CONST(7));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
}

UT_TEST(test_committed_reader_rechecks_namespace_x_lock_after_io)
{
	RelMapFile map;
	RelMapFile zero;
	uint64 generation;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	memset(&map, 0xa5, sizeof(map));
	memset(&zero, 0, sizeof(zero));
	generation = UINT64CONST(99);
	relmap_x_held = true;
	drop_relmap_x_on_read = true;
	UT_ASSERT(!cluster_relmap_authority_v2_read_committed(&identity, &map, &generation));
	relmap_x_held = false;
	UT_ASSERT_EQ(generation, UINT64CONST(0));
	UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)) == 0, true);
}

UT_TEST(test_committed_reader_rebuilds_exact_structural_pending)
{
	ClusterRelmapPendingDescriptor descriptor;
	ClusterRelmapPendingSet pending;
	RelMapFile map;
	RelMapFile zero;
	unsigned char bytes[2176];
	ClusterRelmapPendingResult result;
	uint64 generation;

	memset(&descriptor, 0, sizeof(descriptor));
	descriptor.kind = CLUSTER_RELMAP_PENDING_DROP;
	descriptor.identity.system_identifier = identity.system_identifier;
	descriptor.identity.database_incarnation = identity.database_incarnation;
	memset(descriptor.identity.storage_uuid, 0xd1,
		   sizeof(descriptor.identity.storage_uuid));
	descriptor.identity.locator.spcOid = GLOBALTABLESPACE_OID;
	descriptor.identity.locator.dbOid = InvalidOid;
	descriptor.identity.locator.relNumber = 12001;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	cluster_relmap_pending_init(&pending);
	memset(&map, 0xa5, sizeof(map));
	generation = UINT64CONST(99);
	result = cluster_relmap_authority_v2_read_rebuild_pending(
		&identity, &descriptor, 1, &pending, &map, &generation);
	UT_ASSERT_EQ(result, CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(generation, UINT64CONST(7));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_DROP,
									 &descriptor.identity));
	UT_ASSERT(!cluster_relmap_pending_can_release(&pending, CLUSTER_RELMAP_PENDING_DROP,
									 &descriptor.identity));

	read_copy(0, bytes);
	bytes[128] ^= 1;
	write_copy(0, bytes, sizeof(bytes));
	memset(&pending, 0xa5, sizeof(pending));
	memset(&map, 0xa5, sizeof(map));
	generation = UINT64CONST(99);
	cluster_relmap_pending_init(&pending);
	result = cluster_relmap_authority_v2_read_rebuild_pending(
		&identity, &descriptor, 1, &pending, &map, &generation);
	UT_ASSERT_EQ(result, CLUSTER_RELMAP_PENDING_NOT_READY);
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(generation, UINT64CONST(0));
	UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)) == 0, true);
	UT_ASSERT(!cluster_relmap_pending_contains(&pending, CLUSTER_RELMAP_PENDING_DROP,
									 &descriptor.identity));
}

UT_TEST(test_committed_rebuild_accepts_local_default_and_custom_tablespaces)
{
	ClusterRelmapV2Namespace local = identity;
	ClusterRelmapPendingDescriptor descriptors[2];
	ClusterRelmapPendingSet pending;
	RelMapFile map;
	uint64 generation;

	local.shared = false;
	local.dbid = 42;
	memset(descriptors, 0, sizeof(descriptors));
	for (size_t i = 0; i < lengthof(descriptors); i++) {
		descriptors[i].kind = i == 0
			? CLUSTER_RELMAP_PENDING_DROP : CLUSTER_RELMAP_PENDING_TRUNCATE;
		descriptors[i].identity.system_identifier = local.system_identifier;
		descriptors[i].identity.database_incarnation = local.database_incarnation;
		memset(descriptors[i].identity.storage_uuid, 0xd2 + i,
			   sizeof(descriptors[i].identity.storage_uuid));
		descriptors[i].identity.locator.spcOid = i == 0 ? DEFAULTTABLESPACE_OID : 9000;
		descriptors[i].identity.locator.dbOid = local.dbid;
		descriptors[i].identity.locator.relNumber = 12002 + i;
	}
	setup(&local);
	UT_ASSERT(stage(&local));
	cluster_relmap_pending_init(&pending);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read_rebuild_pending(
		&local, descriptors, lengthof(descriptors), &pending, &map, &generation),
		CLUSTER_RELMAP_PENDING_OK);
	UT_ASSERT_EQ(generation, UINT64CONST(7));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
	for (size_t i = 0; i < lengthof(descriptors); i++) {
		UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptors[i].kind,
			&descriptors[i].identity));
		UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&pending,
			&descriptors[i].identity));
		UT_ASSERT(!cluster_relmap_pending_can_release(&pending, descriptors[i].kind,
			&descriptors[i].identity));
	}
}

UT_TEST(test_committed_rebuild_rejects_foreign_physical_namespace_atomically)
{
	const struct {
		bool shared;
		Oid tablespace;
		Oid database;
	} cases[] = {
		{ true, DEFAULTTABLESPACE_OID, 42 },
		{ true, GLOBALTABLESPACE_OID, 42 },
		{ true, DEFAULTTABLESPACE_OID, InvalidOid },
		{ true, 9000, InvalidOid },
		{ false, DEFAULTTABLESPACE_OID, 43 },
		{ false, DEFAULTTABLESPACE_OID, InvalidOid },
		{ false, GLOBALTABLESPACE_OID, 42 },
		{ false, GLOBALTABLESPACE_OID, InvalidOid },
		{ false, 9000, 43 }
	};

	for (size_t i = 0; i < lengthof(cases); i++) {
		ClusterRelmapV2Namespace ns = identity;
		ClusterRelmapPendingDescriptor descriptors[2];
		RelMapFile zero;

		ns.shared = cases[i].shared;
		ns.dbid = ns.shared ? InvalidOid : 42;
		setup(&ns);
		UT_ASSERT(stage(&ns));
		reset_io();
		memset(descriptors, 0, sizeof(descriptors));
		descriptors[0].identity.system_identifier = ns.system_identifier;
		descriptors[0].identity.database_incarnation = ns.database_incarnation;
		memset(descriptors[0].identity.storage_uuid, 0xd4,
			   sizeof(descriptors[0].identity.storage_uuid));
		descriptors[0].identity.locator.spcOid = ns.shared
			? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
		descriptors[0].identity.locator.dbOid = ns.dbid;
		descriptors[0].identity.locator.relNumber = 12004;
		descriptors[1] = descriptors[0];
		descriptors[1].identity.locator.relNumber++;
		descriptors[1].identity.locator.spcOid = cases[i].tablespace;
		descriptors[1].identity.locator.dbOid = cases[i].database;
		memset(&zero, 0, sizeof(zero));
		for (int kind = CLUSTER_RELMAP_PENDING_DROP;
			 kind <= CLUSTER_RELMAP_PENDING_TRUNCATE; kind++) {
			ClusterRelmapPendingSet pending;
			ClusterRelmapPendingSet before;
			RelMapFile map;
			uint64 generation = UINT64CONST(99);

			descriptors[0].kind = descriptors[1].kind = (ClusterRelmapPendingKind) kind;
			cluster_relmap_pending_init(&pending);
			memcpy(&before, &pending, sizeof(before));
			memset(&map, 0xa5, sizeof(map));
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read_rebuild_pending(
				&ns, descriptors, lengthof(descriptors), &pending, &map, &generation),
				CLUSTER_RELMAP_PENDING_CONFLICT);
			UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
			UT_ASSERT_EQ(generation, UINT64CONST(0));
			UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)), 0);
		}
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(renames, 0);
	}
}

UT_TEST(test_per_database_namespace)
{
	ClusterRelmapV2Namespace local = identity;
	ClusterRelmapV2View view;

	local.shared = false;
	local.dbid = 42;
	setup(&local);
	UT_ASSERT(stage(&local));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&local, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.has_pending && view.pending_complete);
	UT_ASSERT_EQ(view.pending_owner.node, 3);
}

UT_TEST(test_missing_authority_is_never_seeded)
{
	char path[MAXPGPATH];

	setup(&identity);
	for (int i = 0; i < 2; i++) {
		snprintf(path, sizeof(path), "%s/%s", directory, names[i]);
		UT_ASSERT_EQ(unlink(path), 0);
	}
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
	for (int i = 0; i < 2; i++) {
		snprintf(path, sizeof(path), "%s/%s", directory, names[i]);
		UT_ASSERT_EQ(access(path, F_OK), -1);
	}
}

UT_TEST(test_fallback_is_incomplete_and_cannot_stage)
{
	ClusterRelmapV2View view;
	unsigned char bytes[2176];

	setup(&identity);
	read_copy(0, bytes);
	bytes[128] ^= 1;
	write_copy(0, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(view.used_backup && !view.pending_complete && !view.has_pending);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
	write_copy(0, bytes, 100);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
				 CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(!stage(&identity));
}

UT_TEST(test_wrong_identity_format_or_owner_does_not_write)
{
	ClusterRelmapV2Namespace wrong = identity;
	ClusterRelmapV2Owner bad_owner = owner;
	unsigned char bytes[2176];
	ClusterRelmapV2View view;

	setup(&identity);
	wrong.database_incarnation++;
	UT_ASSERT(!stage(&wrong));
	UT_ASSERT(!stage_request(&identity, 9, &owner));
	bad_owner.boot_incarnation = 0;
	UT_ASSERT(!stage_request(&identity, 8, &bad_owner));
	enableFsync = false;
	UT_ASSERT(!stage(&identity));
	enableFsync = true;
	read_copy(0, bytes);
	bytes[0] = 0xa9;
	bytes[1] = 0xde;
	bytes[2] = 0x40;
	bytes[3] = 0x01; /* v1 must not fall back to the valid v2 backup. */
	write_copy(0, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_INVALID);
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
}

UT_TEST(test_existing_pending_is_never_overwritten)
{
	unsigned char before[2176];
	unsigned char after[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	read_copy(0, before);
	UT_ASSERT(!stage(&identity));
	read_copy(0, after);
	UT_ASSERT(memcmp(before, after, sizeof(before)) == 0);
	UT_ASSERT_EQ(renames, 2);
}

UT_TEST(test_symlink_copies_and_temp_are_rejected)
{
	char path[MAXPGPATH];
	char target[MAXPGPATH];
	ClusterRelmapV2View view;

	for (int i = 0; i < 3; i++) {
		setup(&identity);
		snprintf(path, sizeof(path), "%s/%s", directory, names[i]);
		snprintf(target, sizeof(target), "%s/%s", directory, names[1 - (i == 1)]);
		if (i < 2)
			UT_ASSERT_EQ(unlink(path), 0);
		UT_ASSERT_EQ(symlink(target, path), 0);
		if (i < 2)
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
						 CLUSTER_RELMAP_V2_PAIR_INVALID);
		UT_ASSERT(!stage(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_inode_replaced_during_read_is_not_accepted)
{
	ClusterRelmapV2View view;
	ClusterRelmapV2View zero;

	setup(&identity);
	replace_during_read = true;
	memset(&view, 0xa5, sizeof(view));
	memset(&zero, 0, sizeof(zero));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_INVALID);
	UT_ASSERT(memcmp(&view, &zero, sizeof(view)) == 0);
}

UT_TEST(test_each_io_failure_retains_the_obligation)
{
	ClusterRelmapV2View view;

	/* Fail each copy's write/file sync/rename/directory sync in the real path. */
	for (int kind = 0; kind < 4; kind++) {
		for (int copy = 1; copy <= 2; copy++) {
			setup(&identity);
			if (kind == 0)
				fail_write = copy;
			else if (kind == 1)
				fail_file_sync = copy;
			else if (kind == 2)
				fail_rename = copy;
			else
				fail_dir_sync = copy;
			UT_ASSERT(!stage(&identity));
			UT_ASSERT(writer_retention.pins[0].occupied);
			UT_ASSERT(writer_retention.pins[0].may_be_staged);
			UT_ASSERT_EQ(writer_retention.pins[0].wal_end, owner.wal_end);
			LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
			cancelled = cluster_relmap_retention_cancel_prepare(&writer_retention, &writer_key);
			LWLockRelease(&retention_lock);
			UT_ASSERT_EQ(cancelled, CLUSTER_RELMAP_RETENTION_RETAINED);
			UT_ASSERT_EQ(writes_without_pin, 0);
			UT_ASSERT_EQ(io_under_retention_lock, 0);
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
						 CLUSTER_RELMAP_V2_PAIR_VALID);
			UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40000);
			/* Primary replacement is already possible at the first dir sync. */
			UT_ASSERT_EQ(view.has_pending, copy == 2 || kind == 3);
		}
	}
}

UT_TEST(test_corrupt_or_replaced_readback_never_acknowledges_stage)
{
	setup(&identity);
	corrupt_after_rename = true;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(renames, 1);
	setup(&identity);
	replace_after_rename = true;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(renames, 1);
}

static ClusterRelmapV2Image
staged_image(void)
{
	ClusterRelmapV2Image image;

	memset(&image, 0, sizeof(image));
	image.identity = identity;
	image.committed_generation = 7;
	image.committed = map_image(40000);
	image.pending_generation = 8;
	image.pending_owner = owner;
	image.pending = map_image(40001);
	return image;
}

static ClusterRelmapV2ClearResult
finish(const ClusterRelmapV2Image *image, bool publish)
{
	ClusterRelmapV2ClearResult result;

	relmap_x_held = true;
	result = publish ? cluster_relmap_authority_v2_publish(image)
					 : cluster_relmap_authority_v2_discard_pending(image);
	relmap_x_held = false;
	return result;
}

UT_TEST(test_v2_mutations_require_namespace_x_lock)
{
	RelMapFile map = map_image(40001);
	ClusterRelmapV2Image image = staged_image();

	setup(&identity);
	observing_stage = true;
	UT_ASSERT(!cluster_relmap_authority_v2_write_pending(&identity, 8, &owner, &map,
		&writer_retention, &retention_lock));
	observing_stage = false;
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_publish(&image), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_discard_pending(&image), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(reads, 0);
	UT_ASSERT_EQ(writes, 0);
}

static bool
clean_pair(bool published)
{
	unsigned char primary[2176];
	unsigned char backup[2176];
	ClusterRelmapV2Image disk;
	RelMapFile expected = map_image(published ? 40001 : 40000);

	read_copy(0, primary);
	read_copy(1, backup);
	if (memcmp(primary, backup, sizeof(primary)) != 0
		|| cluster_relmap_v2_decode(primary, sizeof(primary), &identity, &disk)
			   != CLUSTER_RELMAP_V2_VALID
		|| disk.committed_generation != (published ? 8 : 7) || disk.pending_generation != 0
		|| memcmp(&disk.committed, &expected, sizeof(expected)) != 0)
		return false;
	for (int i = 54; i < 96; i++)
		if (primary[i] != 0)
			return false;
	for (int i = 1152; i < 2176; i++)
		if (primary[i] != 0)
			return false;
	return true;
}

UT_TEST(test_publish_and_discard_clear_both_copies)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2View view;

	for (int publish = 0; publish <= 1; publish++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		reset_io();
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT(clean_pair(publish));
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.pending_complete && !view.has_pending);
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
	}
}

UT_TEST(test_partial_stage_can_be_discarded)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2View view;

	setup(&identity);
	fail_write = 2;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT(view.has_pending && view.pending_complete);
	reset_io();
	UT_ASSERT_EQ(finish(&image, false), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	UT_ASSERT(clean_pair(false));
}

UT_TEST(test_primary_done_keeps_backup_pending_until_distinct_retry)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2View view;

	for (int publish = 0; publish <= 1; publish++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		reset_io();
		fail_rename = 2;
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
					 CLUSTER_RELMAP_V2_PAIR_VALID);
		UT_ASSERT(view.has_pending && view.pending_complete);
		UT_ASSERT_EQ(view.pending_owner.top_full_xid, UINT64CONST(0x200000123));
		UT_ASSERT_EQ(view.committed_generation, publish ? 8 : 7);
		/* A new recovery attempt, never success for the original failed call. */
		reset_io();
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT(clean_pair(publish));
	}
}

UT_TEST(test_exact_stage_identity_and_images_required_before_writes)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2Image wrong;
	unsigned char before[2176];
	unsigned char after[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	read_copy(0, before);
	reset_io();
	for (int change = 0; change < 15; change++) {
		wrong = image;
		switch (change) {
		case 0: wrong.identity.system_identifier++; break;
		case 1: wrong.identity.database_incarnation++; break;
		case 2: wrong.identity.dbid = 42; wrong.identity.shared = false; break;
		case 3: wrong.pending_owner.node++; break;
		case 4: wrong.pending_owner.top_full_xid += UINT64CONST(0x100000000); break;
		case 5: wrong.pending_owner.membership_epoch++; break;
		case 6: wrong.pending_owner.admitted_incarnation++; break;
		case 7: wrong.pending_owner.boot_incarnation++; break;
		case 8: wrong.pending_owner.wal_end++; break;
		case 9: wrong.committed_generation++; wrong.pending_generation++; break;
		case 10: wrong.committed = map_image(41000); break;
		case 11: wrong.pending = map_image(41001); break;
		case 12: wrong.pending_owner.boot_incarnation = 0; break;
		case 13: wrong.pending_generation = 0; break;
		case 14: wrong.committed_generation = PG_UINT64_MAX; wrong.pending_generation = 0; break;
		}
		for (int publish = 0; publish <= 1; publish++)
			UT_ASSERT_EQ(finish(&wrong, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	}
	UT_ASSERT_EQ(finish(NULL, false), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(finish(NULL, true), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(writes, 0);
	read_copy(0, after);
	UT_ASSERT(memcmp(before, after, sizeof(before)) == 0);
	read_copy(1, after);
	UT_ASSERT(memcmp(before, after, sizeof(before)) == 0);
}

UT_TEST(test_unknown_or_conflicting_copy_never_grants_cleanup)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2Image wrong;
	unsigned char bytes[2176];
	char path[MAXPGPATH];

	for (int kind = 0; kind < 6; kind++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		read_copy(1, bytes);
		if (kind == 0)
			bytes[128] ^= 1;
		else if (kind == 1) {
			snprintf(path, sizeof(path), "%s/%s", directory, names[1]);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (kind == 2)
			bytes[3] = 0x01;
		else {
			wrong = image;
			if (kind == 3)
				wrong.pending_owner.boot_incarnation++;
			else if (kind == 4)
				wrong.committed = map_image(41000);
			else
				wrong.identity.database_incarnation++;
			UT_ASSERT_EQ(cluster_relmap_v2_encode(&wrong, bytes, sizeof(bytes)),
						 CLUSTER_RELMAP_V2_VALID);
		}
		if (kind != 1)
			write_copy(1, bytes, sizeof(bytes));
		reset_io();
		UT_ASSERT_EQ(finish(&image, false), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(finish(&image, true), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_each_finish_io_failure_is_retained_unknown)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2View view;

	for (int publish = 0; publish <= 1; publish++) {
		for (int kind = 0; kind < 4; kind++) {
			for (int copy = 1; copy <= 2; copy++) {
				setup(&identity);
				UT_ASSERT(stage(&identity));
				reset_io();
				if (kind == 0)
					fail_write = copy;
				else if (kind == 1)
					fail_file_sync = copy;
				else if (kind == 2)
					fail_rename = copy;
				else
					fail_dir_sync = copy;
				UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
				UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view),
							 CLUSTER_RELMAP_V2_PAIR_VALID);
				UT_ASSERT_EQ(view.committed_generation,
							 publish && (copy == 2 || kind == 3) ? 8 : 7);
				UT_ASSERT_EQ(view.has_pending, !(copy == 2 && kind == 3));
			}
		}
	}
}

UT_TEST(test_already_clean_call_requires_new_sync_and_postread)
{
	ClusterRelmapV2Image image = staged_image();

	for (int publish = 0; publish <= 1; publish++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		reset_io();
		/* Both names are clean but the second directory sync failed. */
		fail_dir_sync = 2;
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT(clean_pair(publish));
		reset_io();
		fail_file_sync = 1;
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(renames, 0);
		reset_io();
		enableFsync = false;
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT_EQ(writes, 0);
		enableFsync = true;
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
		UT_ASSERT(clean_pair(publish));
		UT_ASSERT_EQ(file_syncs, 2);
		UT_ASSERT_EQ(dir_syncs, 2);
	}
}

UT_TEST(test_finish_rechecks_primary_after_backup_install)
{
	ClusterRelmapV2Image image = staged_image();

	for (int publish = 0; publish <= 1; publish++) {
		for (int copy = 1; copy <= 2; copy++) {
			for (int replace = 0; replace <= 1; replace++) {
				setup(&identity);
				UT_ASSERT(stage(&identity));
				reset_io();
				fault_rename = copy;
				corrupt_after_rename = !replace;
				replace_after_rename = replace;
				UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			}
		}
	}
}

UT_TEST(test_directory_close_failure_cannot_acknowledge_clean_pair)
{
	ClusterRelmapV2Image image = staged_image();

	for (int publish = 0; publish <= 1; publish++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		reset_io();
		fail_directory_close = true;
		UT_ASSERT_EQ(finish(&image, publish), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
		UT_ASSERT(clean_pair(publish));
	}
}

UT_TEST(test_clean_unstaged_pair_cannot_fabricate_publish)
{
	ClusterRelmapV2Image image = staged_image();
	ClusterRelmapV2Image later = image;

	setup(&identity);
	UT_ASSERT_EQ(finish(&image, true), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(stage(&identity));
	UT_ASSERT_EQ(finish(&image, true), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	reset_io();
	/* A stale discard must never undo the already-published committed map. */
	UT_ASSERT_EQ(finish(&image, false), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	later.committed_generation = 8;
	later.committed = map_image(40001);
	later.pending_generation = 9;
	later.pending = map_image(40002);
	UT_ASSERT_EQ(finish(&later, true), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT(clean_pair(true));
}

UT_TEST(test_pending_reader_preserves_base_for_cold_finish)
{
	ClusterRelmapV2Image original = staged_image();
	ClusterRelmapV2Image recovered;
	ClusterRelmapV2View view;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	reset_io();
	fail_rename = 2;
	UT_ASSERT_EQ(finish(&original, true), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
	UT_ASSERT_EQ(view.committed_generation, 8);
	UT_ASSERT_EQ(view.committed.mappings[0].mapfilenumber, 40001);
	UT_ASSERT_EQ(view.pending_committed_generation, 7);
	UT_ASSERT_EQ(view.pending_committed.mappings[0].mapfilenumber, 40000);
	memset(&recovered, 0, sizeof(recovered));
	recovered.identity = identity;
	recovered.committed_generation = view.pending_committed_generation;
	recovered.committed = view.pending_committed;
	recovered.pending_generation = view.pending_generation;
	recovered.pending_owner = view.pending_owner;
	recovered.pending = view.pending;
	reset_io();
	UT_ASSERT_EQ(finish(&recovered, true), CLUSTER_RELMAP_V2_CLEARED_POSTREAD);
	UT_ASSERT(clean_pair(true));
}

static void
v1_header_crc(unsigned char *bytes, const ClusterRelmapAuthorityHeader *header)
{
	ClusterRelmapAuthorityHeader hdr = *header;

	memcpy(bytes, &hdr, sizeof(hdr));
	INIT_CRC32C(hdr.crc);
	COMP_CRC32C(hdr.crc, bytes, offsetof(ClusterRelmapAuthorityHeader, crc));
	FIN_CRC32C(hdr.crc);
	memcpy(bytes, &hdr, sizeof(hdr));
}

static void
v1_image(const ClusterRelmapV2Namespace *ns, unsigned char *bytes)
{
	ClusterRelmapAuthorityHeader hdr = { 0 };
	RelMapFile map = map_image(40000);

	memset(bytes, 0, CLUSTER_RELMAP_AUTHORITY_FILE_SIZE);
	hdr.magic = CLUSTER_RELMAP_AUTHORITY_MAGIC;
	hdr.version = 1;
	hdr.committed_generation = 7;
	hdr.dbid = ns->dbid;
	hdr.shared_map = ns->shared;
	hdr.image_size = sizeof(map);
	v1_header_crc(bytes, &hdr);
	memcpy(bytes + CLUSTER_RELMAP_COMMITTED_OFFSET, &map, sizeof(map));
}

static void
setup_v1(const ClusterRelmapV2Namespace *ns)
{
	unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];

	setup(ns);
	v1_image(ns, bytes);
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	relmap_x_held = true;
}

static bool
conversion_pair_clean(const ClusterRelmapV2Namespace *ns)
{
	ClusterRelmapV2View view;
	unsigned char a[2176], b[2176];

	if (cluster_relmap_authority_v2_read(ns, &view) != CLUSTER_RELMAP_V2_PAIR_VALID
		|| view.has_pending || !view.pending_complete || view.committed_generation != 7
		|| view.committed.mappings[0].mapfilenumber != 40000)
		return false;
	read_copy(0, a);
	read_copy(1, b);
	return memcmp(a, b, sizeof(a)) == 0
		&& cluster_relmap_authority_classify((char *)a, sizeof(a))
			!= CLUSTER_RELMAP_AUTHORITY_VALID
		&& cluster_relmap_authority_classify((char *)b, sizeof(b))
			!= CLUSTER_RELMAP_AUTHORITY_VALID;
}

UT_TEST(test_conversion_syncs_both_and_disables_old_reader)
{
	char old_map[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 old_length;
	RelMapFile map;
	uint64 generation;

	setup_v1(&identity);
	UT_ASSERT(cluster_relmap_authority_v2_convert_clean(&identity));
	UT_ASSERT_EQ(renames, 2);
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
	UT_ASSERT(conversion_pair_clean(&identity));
	UT_ASSERT(!cluster_relmap_authority_read_committed(true, InvalidOid, old_map, &old_length));
	UT_ASSERT(cluster_relmap_authority_v2_read_committed(&identity, &map, &generation));
	UT_ASSERT_EQ(generation, UINT64CONST(7));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
}

UT_TEST(test_conversion_local_namespace_and_inactive_v1_bytes)
{
	ClusterRelmapV2Namespace local = identity;
	ClusterRelmapAuthorityHeader hdr;
	unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];

	local.shared = false;
	local.dbid = 42;
	setup_v1(&local);
	v1_image(&local, bytes);
	memcpy(&hdr, bytes, sizeof(hdr));
	/* The old publish leaves these inactive bytes behind at pending_gen=0. */
	hdr.owner_node = 3;
	hdr.owner_xid = 291;
	hdr.owner_epoch = 101;
	hdr.relmap_lsn = 909;
	v1_header_crc(bytes, &hdr);
	memset(bytes + CLUSTER_RELMAP_PENDING_OFFSET, 0xd1, CLUSTER_RELMAP_IMAGE_MAX);
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	UT_ASSERT(cluster_relmap_authority_v2_convert_clean(&local));
	UT_ASSERT(conversion_pair_clean(&local));
}

UT_TEST(test_conversion_never_resolves_v1_pending)
{
	unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader hdr;

	for (int which = 0; which < 3; which++) {
		setup_v1(&identity);
		v1_image(&identity, bytes);
		memcpy(&hdr, bytes, sizeof(hdr));
		hdr.pending_generation = 8;
		hdr.owner_node = 3;
		hdr.owner_xid = 291;
		v1_header_crc(bytes, &hdr);
		write_copy(which == 1 ? 1 : 0, bytes, sizeof(bytes));
		if (which == 2)
			write_copy(1, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_conversion_validates_exact_v1_header_and_native_image)
{
	unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader hdr;
	RelMapFile map;

	for (int change = 0; change < 12; change++) {
		setup_v1(&identity);
		v1_image(&identity, bytes);
		memcpy(&hdr, bytes, sizeof(hdr));
		switch (change) {
		case 0: hdr.version = 2; break;
		case 1: hdr.magic++; break;
		case 2: hdr.image_size--; break;
		case 3: hdr.dbid = 42; break;
		case 4: hdr.shared_map = 2; break;
		case 5: hdr.committed_generation = 0; break;
		case 6: hdr.pad0 = 1; break;
		case 7: hdr.pad1 = 1; break;
		case 8: bytes[CLUSTER_RELMAP_COMMITTED_OFFSET] ^= 1; break;
		case 9:
			map = map_image(40000);
			map.mappings[0].mapoid = InvalidOid;
			INIT_CRC32C(map.crc);
			COMP_CRC32C(map.crc, &map, offsetof(RelMapFile, crc));
			FIN_CRC32C(map.crc);
			memcpy(bytes + CLUSTER_RELMAP_COMMITTED_OFFSET, &map, sizeof(map));
			break;
		case 10: bytes[CLUSTER_RELMAP_COMMITTED_OFFSET + sizeof(RelMapFile)] = 1; break;
		case 11: break;
		}
		v1_header_crc(bytes, &hdr);
		if (change == 11)
			bytes[offsetof(ClusterRelmapAuthorityHeader, crc)] ^= 1;
		write_copy(0, bytes, sizeof(bytes));
		write_copy(1, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_conversion_rejects_conflicting_v1_copies)
{
	unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE];
	ClusterRelmapAuthorityHeader hdr;
	RelMapFile map;

	for (int change = 0; change < 3; change++) {
		setup_v1(&identity);
		v1_image(&identity, bytes);
		memcpy(&hdr, bytes, sizeof(hdr));
		if (change == 0)
			hdr.committed_generation++;
		else if (change == 1) {
			map = map_image(40001);
			memcpy(bytes + CLUSTER_RELMAP_COMMITTED_OFFSET, &map, sizeof(map));
		} else
			hdr.owner_xid = 291;
		v1_header_crc(bytes, &hdr);
		write_copy(1, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_conversion_backup_is_replaced_before_primary)
{
	char old_map[CLUSTER_RELMAP_IMAGE_MAX];
	uint32 old_length;
	RelMapFile map;
	unsigned char backup[2176];
	ClusterRelmapV2Image converted;
	ClusterRelmapV2View view;
	uint64 generation = 99;

	setup_v1(&identity);
	fail_write = 2;
	UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
	UT_ASSERT_EQ(renames, 1);
	UT_ASSERT_EQ(file_syncs, 1);
	UT_ASSERT_EQ(dir_syncs, 1);
	if (renames != 1)
		return;
	read_copy(1, backup);
	UT_ASSERT_EQ(cluster_relmap_v2_decode(backup, sizeof(backup), &identity, &converted),
		CLUSTER_RELMAP_V2_VALID);
	UT_ASSERT(!converted.pending_generation);
	UT_ASSERT(cluster_relmap_authority_read_committed(true, InvalidOid, old_map, &old_length));
	memcpy(&map, old_map, sizeof(map));
	UT_ASSERT_EQ(map.mappings[0].mapfilenumber, 40000);
	/* Raw inspection can report a degraded backup, but no committed runtime
	 * reader or publisher may consume this mixed conversion cut. */
	UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&identity, &view), CLUSTER_RELMAP_V2_PAIR_DEGRADED);
	UT_ASSERT(view.used_backup && !view.pending_complete);
	UT_ASSERT(!cluster_relmap_authority_v2_read_committed(&identity, &map, &generation));
	UT_ASSERT_EQ(generation, 0);
	UT_ASSERT_EQ(map.magic, 0);
	UT_ASSERT(!stage(&identity));
	reset_io();
	relmap_x_held = true;
	UT_ASSERT(cluster_relmap_authority_v2_convert_clean(&identity));
	UT_ASSERT(conversion_pair_clean(&identity));
}

UT_TEST(test_conversion_mixed_pair_requires_exact_clean_matching_state)
{
	ClusterRelmapV2Image image;
	unsigned char bytes[2176];

	for (int change = 0; change < 7; change++) {
		setup_v1(&identity);
		memset(&image, 0, sizeof(image));
		image.identity = identity;
		image.committed_generation = 7;
		image.committed = map_image(40000);
		if (change == 1)
			image.committed_generation++;
		else if (change == 2)
			image.committed = map_image(40001);
		else if (change == 3)
			image.identity.database_incarnation++;
		else if (change == 4)
			image.identity.system_identifier++;
		else if (change == 5) {
			image.pending_generation = 8;
			image.pending_owner = owner;
			image.pending = map_image(40001);
		}
		UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
		if (change == 6)
			bytes[128] ^= 1;
		/* A v2 primary/v1 backup cannot be made by backup-first conversion. */
		write_copy(change == 0 ? 0 : 1, bytes, sizeof(bytes));
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_conversion_io_failures_require_a_distinct_successful_retry)
{
	for (int kind = 0; kind < 4; kind++) {
		for (int copy = 1; copy <= 2; copy++) {
			setup_v1(&identity);
			if (kind == 0) fail_write = copy;
			else if (kind == 1) fail_file_sync = copy;
			else if (kind == 2) fail_rename = copy;
			else fail_dir_sync = copy;
			UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
			reset_io();
			relmap_x_held = true;
			UT_ASSERT(cluster_relmap_authority_v2_convert_clean(&identity));
			UT_ASSERT(conversion_pair_clean(&identity));
			UT_ASSERT_EQ(file_syncs, 2);
			UT_ASSERT_EQ(dir_syncs, 2);
		}
	}
}

UT_TEST(test_conversion_already_v2_retry_still_requires_sync)
{
	setup(&identity);
	relmap_x_held = true;
	fail_file_sync = 1;
	UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
	UT_ASSERT_EQ(renames, 0);
	reset_io();
	relmap_x_held = true;
	UT_ASSERT(cluster_relmap_authority_v2_convert_clean(&identity));
	UT_ASSERT(conversion_pair_clean(&identity));
	UT_ASSERT_EQ(file_syncs, 2);
	UT_ASSERT_EQ(dir_syncs, 2);
}

UT_TEST(test_conversion_refuses_missing_short_overlong_and_unsafe_files)
{
	unsigned char bytes[CLUSTER_RELMAP_AUTHORITY_FILE_SIZE + 1];
	char path[MAXPGPATH];

	for (int change = 0; change < 7; change++) {
		setup_v1(&identity);
		snprintf(path, sizeof(path), "%s/%s", directory, names[change == 0 ? 0 : 1]);
		if (change < 2)
			UT_ASSERT_EQ(unlink(path), 0);
		else if (change < 4) {
			v1_image(&identity, bytes);
			bytes[sizeof(bytes) - 1] = 0;
			write_copy(1, bytes, change == 2 ? 100 : sizeof(bytes));
		} else if (change == 4) {
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(symlink(names[0], path), 0);
		} else if (change == 5) {
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(mkfifo(path, 0600), 0);
		} else {
			snprintf(path, sizeof(path), "%s/%s", directory, names[3]);
			UT_ASSERT_EQ(symlink(names[0], path), 0);
		}
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_conversion_requires_identity_sync_policy_and_continuous_namespace_lock)
{
	ClusterRelmapV2Namespace invalid = identity;

	for (int change = 0; change < 6; change++) {
		setup_v1(&identity);
		invalid = identity;
		if (change == 0) relmap_x_held = false;
		else if (change == 1) enableFsync = false;
		else if (change == 2) invalid.system_identifier = 0;
		else if (change == 3) invalid.dbid = 42;
		else if (change == 4) drop_relmap_x_on_read = true;
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(change == 5 ? NULL : &invalid));
		UT_ASSERT_EQ(writes, 0);
	}
}

UT_TEST(test_conversion_postread_corruption_replacement_and_close_cannot_acknowledge)
{
	for (int change = 0; change < 4; change++) {
		setup_v1(&identity);
		fault_rename = 2;
		if (change < 2) {
			corrupt_after_rename = true;
			fault_copy = change;
		} else if (change == 2)
			replace_after_rename = true;
		else
			fail_directory_close = true;
		UT_ASSERT(!cluster_relmap_authority_v2_convert_clean(&identity));
		if (change == 3) {
			UT_ASSERT(conversion_pair_clean(&identity));
			reset_io();
			relmap_x_held = true;
			UT_ASSERT(cluster_relmap_authority_v2_convert_clean(&identity));
		}
	}
}

#define RESTORE_BASE UINT64CONST(0x200000000)
#define RESTORE_OK CLUSTER_RELMAP_RETENTION_OK

static void
restore_builders(ClusterRelmapRetentionDirectory storage[2],
	ClusterRelmapRetentionDirectory *directories[2])
{
	memset(storage, 0, 2 * sizeof(*storage));
	for (int i = 0; i < 2; i++) {
		directories[i] = &storage[i];
		if (cluster_relmap_retention_build_start(directories[i], identity.system_identifier,
			identity.database_incarnation, i == 0 ? 7 : owner.node, 99,
			RESTORE_BASE + 3, RESTORE_BASE + 1000) != RESTORE_OK)
			abort();
	}
}

static size_t
pin_count(const ClusterRelmapRetentionDirectory *directory)
{
	size_t count = 0;

	for (size_t i = 0; i < lengthof(directory->pins); i++)
		count += directory->pins[i].occupied;
	return count;
}

static bool
restore_refused(ClusterRelmapRetentionDirectory storage[2])
{
	ClusterRelmapRetentionTruncate cut;

	for (int i = 0; i < 2; i++)
		if (storage[i].state != CLUSTER_RELMAP_RETENTION_UNKNOWN
			|| cluster_relmap_retention_build_finish(&storage[i]) == RESTORE_OK
			|| cluster_relmap_retention_allocation_check(&storage[i], RESTORE_BASE + 1001)
				== RESTORE_OK
			|| cluster_relmap_retention_truncate_begin(&storage[i], RESTORE_BASE + 500, &cut)
				== RESTORE_OK)
			return false;
	return true;
}

UT_TEST(test_restore_clean_pair_is_only_one_building_observation)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapRetentionTruncate cut;

	setup(&identity);
	restore_builders(storage, directories);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	UT_ASSERT_EQ(reads, 2);
	UT_ASSERT_EQ(writes, 0);
	for (int i = 0; i < 2; i++) {
		UT_ASSERT_EQ(storage[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
		UT_ASSERT_EQ(pin_count(&storage[i]), 0);
		UT_ASSERT_NE(cluster_relmap_retention_allocation_check(&storage[i], RESTORE_BASE + 1001), RESTORE_OK);
		UT_ASSERT_NE(cluster_relmap_retention_truncate_begin(&storage[i], RESTORE_BASE + 500, &cut), RESTORE_OK);
	}
}

UT_TEST(test_restore_exact_original_origin_and_epoch_zero)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapRetentionPin *pin;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	reset_io();
	restore_builders(storage, directories);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	UT_ASSERT_EQ(pin_count(&storage[0]), 0);
	UT_ASSERT_EQ(pin_count(&storage[1]), 1);
	pin = &storage[1].pins[0];
	UT_ASSERT_EQ(pin->key.identity.system_identifier, identity.system_identifier);
	UT_ASSERT_EQ(pin->key.identity.database_incarnation, identity.database_incarnation);
	UT_ASSERT(pin->key.identity.shared && pin->key.identity.dbid == InvalidOid);
	UT_ASSERT_EQ(pin->key.origin, owner.node);
	UT_ASSERT_EQ(pin->key.generation, 8);
	UT_ASSERT_EQ(pin->key.top_full_xid, owner.top_full_xid);
	UT_ASSERT_EQ(pin->key.membership_epoch, owner.membership_epoch);
	UT_ASSERT_EQ(pin->key.admitted_incarnation, owner.admitted_incarnation);
	UT_ASSERT_EQ(pin->key.boot_incarnation, owner.boot_incarnation);
	UT_ASSERT_EQ(pin->wal_end, owner.wal_end);
	UT_ASSERT(pin->occupied && pin->may_be_staged);
	UT_ASSERT_EQ(storage[0].state, CLUSTER_RELMAP_RETENTION_BUILDING);
	UT_ASSERT_EQ(storage[1].state, CLUSTER_RELMAP_RETENTION_BUILDING);
	UT_ASSERT_EQ(writes, 0);
}

UT_TEST(test_committed_rebuild_at_legal_stage_publish_discard_cuts)
{
	for (int shared = 0; shared <= 1; shared++) {
		ClusterRelmapV2Namespace ns = identity;
		ClusterRelmapV2Image image = staged_image();
		ClusterRelmapPendingDescriptor descriptors[2];

		ns.shared = shared;
		ns.dbid = shared ? InvalidOid : 42;
		image.identity = ns;
		memset(descriptors, 0, sizeof(descriptors));
		for (size_t i = 0; i < lengthof(descriptors); i++) {
			descriptors[i].kind = i == 0
				? CLUSTER_RELMAP_PENDING_DROP : CLUSTER_RELMAP_PENDING_TRUNCATE;
			descriptors[i].identity.system_identifier = ns.system_identifier;
			descriptors[i].identity.database_incarnation = ns.database_incarnation;
			memset(descriptors[i].identity.storage_uuid, 0xd6 + i,
				   sizeof(descriptors[i].identity.storage_uuid));
			descriptors[i].identity.locator.spcOid = shared
				? GLOBALTABLESPACE_OID : (i == 0 ? DEFAULTTABLESPACE_OID : 9000);
			descriptors[i].identity.locator.dbOid = ns.dbid;
			descriptors[i].identity.locator.relNumber = 12006 + i;
		}
		for (int kind = 0; kind < 3; kind++) {
			ClusterRelmapRetentionDirectory storage[2], *directories[2];
			ClusterRelmapRetentionPin *pin;
			ClusterRelmapV2View view;
			ClusterRelmapPendingSet pending;
			RelMapFile map;
			RelMapFile expected = map_image(kind == 1 ? 40001 : 40000);
			unsigned char before[2][2176], after[2176];
			uint64 generation = UINT64CONST(99);

			setup(&ns);
			if (kind == 0) {
				fail_rename = 2;
				UT_ASSERT(!stage(&ns));
			} else {
				UT_ASSERT(stage(&ns));
				reset_io();
				fail_rename = 2;
				UT_ASSERT_EQ(finish(&image, kind == 1), CLUSTER_RELMAP_V2_RETAINED_UNKNOWN);
			}
			/* Original I/O produced the cut; readers must not repair its files. */
			read_copy(0, before[0]);
			read_copy(1, before[1]);
			UT_ASSERT_NE(memcmp(before[0], before[1], sizeof(before[0])), 0);
			reset_io();
			memset(&map, 0xa5, sizeof(map));
			UT_ASSERT(read_committed_under_x(&ns, &map, &generation));
			UT_ASSERT_EQ(generation, kind == 1 ? 8 : 7);
			UT_ASSERT_EQ(memcmp(&map, &expected, sizeof(map)), 0);
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&ns, &view), CLUSTER_RELMAP_V2_PAIR_VALID);
			UT_ASSERT(view.pending_complete && view.has_pending);
			UT_ASSERT_EQ(view.pending_committed_generation, 7);
			UT_ASSERT_EQ(view.pending_committed.mappings[0].mapfilenumber, 40000);
			UT_ASSERT_EQ(view.pending_generation, 8);
			UT_ASSERT_EQ(view.pending.mappings[0].mapfilenumber, 40001);

			cluster_relmap_pending_init(&pending);
			memset(&map, 0xa5, sizeof(map));
			generation = UINT64CONST(99);
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read_rebuild_pending(
				&ns, descriptors, lengthof(descriptors), &pending, &map, &generation),
				CLUSTER_RELMAP_PENDING_OK);
			UT_ASSERT_EQ(generation, kind == 1 ? 8 : 7);
			UT_ASSERT_EQ(memcmp(&map, &expected, sizeof(map)), 0);
			for (size_t i = 0; i < lengthof(descriptors); i++) {
				UT_ASSERT(cluster_relmap_pending_contains(&pending, descriptors[i].kind,
					&descriptors[i].identity));
				UT_ASSERT(cluster_relmap_pending_rel_number_reserved(&pending,
					&descriptors[i].identity));
				UT_ASSERT(!cluster_relmap_pending_can_release(&pending, descriptors[i].kind,
					&descriptors[i].identity));
			}
			for (size_t i = 0; i < lengthof(pending.entries); i++) {
				const ClusterRelmapPendingEntry *entry = &pending.entries[i];

				if (!entry->occupied)
					continue;
				UT_ASSERT(entry->main_placeholder);
				UT_ASSERT(!entry->copies_cleared_readback && !entry->native_terminal
					&& !entry->tt_complete && !entry->space_complete);
			}
			restore_builders(storage, directories);
			UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&ns, directories, 2), RESTORE_OK);
			UT_ASSERT_EQ(pin_count(&storage[0]), 0);
			UT_ASSERT_EQ(pin_count(&storage[1]), 1);
			pin = &storage[1].pins[0];
			UT_ASSERT(pin->occupied && pin->may_be_staged);
			UT_ASSERT_EQ(pin->key.identity.shared, ns.shared);
			UT_ASSERT_EQ(pin->key.identity.dbid, ns.dbid);
			UT_ASSERT_EQ(pin->key.generation, 8);
			UT_ASSERT_EQ(pin->key.origin, owner.node);
			UT_ASSERT_EQ(pin->key.top_full_xid, owner.top_full_xid);
			UT_ASSERT_EQ(pin->key.membership_epoch, owner.membership_epoch);
			UT_ASSERT_EQ(pin->key.admitted_incarnation, owner.admitted_incarnation);
			UT_ASSERT_EQ(pin->key.boot_incarnation, owner.boot_incarnation);
			UT_ASSERT_EQ(pin->wal_end, owner.wal_end);
			for (int i = 0; i < 2; i++) {
				UT_ASSERT_EQ(storage[i].state, CLUSTER_RELMAP_RETENTION_BUILDING);
				read_copy(i, after);
				UT_ASSERT_EQ(memcmp(before[i], after, sizeof(after)), 0);
			}
			UT_ASSERT_EQ(writes, 0);
			UT_ASSERT_EQ(renames, 0);
			UT_ASSERT_EQ(file_syncs, 0);
			UT_ASSERT_EQ(dir_syncs, 0);
		}
	}
}

UT_TEST(test_committed_rebuild_rejects_valid_crc_pair_conflicts)
{
	for (int shared = 0; shared <= 1; shared++) {
		for (int change = 0; change < 5; change++) {
			ClusterRelmapV2Namespace ns = identity;
			ClusterRelmapV2Image other = staged_image();
			ClusterRelmapV2Image checked;
			ClusterRelmapV2View view;
			ClusterRelmapPendingSet pending, before;
			RelMapFile map, zero;
			unsigned char copies[2][2176], after[2176];
			uint64 generation = UINT64CONST(99);

			ns.shared = shared;
			ns.dbid = shared ? InvalidOid : 42;
			other.identity = ns;
			setup(&ns);
			UT_ASSERT(stage(&ns));
			switch (change) {
			case 0: other.committed = map_image(40002); break;
			case 1: other.pending_owner.top_full_xid += UINT64CONST(0x100000000); break;
			case 2: other.pending_owner.boot_incarnation++; break;
			case 3: other.pending = map_image(40002); break;
			case 4:
				/* Publish may advance primary first, never backup alone. */
				other.committed_generation = 8;
				other.committed = map_image(40001);
				other.pending_generation = 0;
				memset(&other.pending_owner, 0, sizeof(other.pending_owner));
				memset(&other.pending, 0, sizeof(other.pending));
				break;
			}
			UT_ASSERT_EQ(cluster_relmap_v2_encode(&other, copies[1], sizeof(copies[1])),
				CLUSTER_RELMAP_V2_VALID);
			UT_ASSERT_EQ(cluster_relmap_v2_decode(copies[1], sizeof(copies[1]), &ns, &checked),
				CLUSTER_RELMAP_V2_VALID);
			write_copy(1, copies[1], sizeof(copies[1]));
			read_copy(0, copies[0]);
			reset_io();
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read(&ns, &view), CLUSTER_RELMAP_V2_PAIR_CONFLICT);
			memset(&map, 0xa5, sizeof(map));
			memset(&zero, 0, sizeof(zero));
			UT_ASSERT(!read_committed_under_x(&ns, &map, &generation));
			UT_ASSERT_EQ(generation, UINT64CONST(0));
			UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)), 0);
			cluster_relmap_pending_init(&pending);
			memcpy(&before, &pending, sizeof(before));
			memset(&map, 0xa5, sizeof(map));
			generation = UINT64CONST(99);
			UT_ASSERT_EQ(cluster_relmap_authority_v2_read_rebuild_pending(
				&ns, NULL, 0, &pending, &map, &generation), CLUSTER_RELMAP_PENDING_NOT_READY);
			UT_ASSERT_EQ(memcmp(&pending, &before, sizeof(pending)), 0);
			UT_ASSERT_EQ(generation, UINT64CONST(0));
			UT_ASSERT_EQ(memcmp(&map, &zero, sizeof(map)), 0);
			for (int i = 0; i < 2; i++) {
				read_copy(i, after);
				UT_ASSERT_EQ(memcmp(copies[i], after, sizeof(after)), 0);
			}
			UT_ASSERT_EQ(writes, 0);
			UT_ASSERT_EQ(renames, 0);
		}
	}
}

UT_TEST(test_restore_incomplete_corrupt_foreign_and_conflicting_pairs_poison_build)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	unsigned char bytes[2176];
	char path[MAXPGPATH];

	for (int kind = 0; kind < 5; kind++) {
		ClusterRelmapV2Image image = staged_image();

		setup(&identity);
		UT_ASSERT(stage(&identity));
		read_copy(1, bytes);
		if (kind == 0) {
			snprintf(path, sizeof(path), "%s/%s", directory, names[1]);
			UT_ASSERT_EQ(unlink(path), 0);
		} else if (kind == 1)
			write_copy(1, bytes, 40);
		else if (kind == 2) {
			bytes[128] ^= 1;
			write_copy(1, bytes, sizeof(bytes));
		} else {
			if (kind == 3)
				image.identity.database_incarnation++;
			else
				image.pending_owner.boot_incarnation++;
			UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
			write_copy(1, bytes, sizeof(bytes));
		}
		restore_builders(storage, directories);
		UT_ASSERT_NE(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
		UT_ASSERT(restore_refused(storage));
	}
}

UT_TEST(test_restore_requires_qualified_distinct_original_origin_builders)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];

	for (int kind = 0; kind < 6; kind++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		reset_io();
		restore_builders(storage, directories);
		switch (kind) {
		case 0: storage[1].origin = 8; break;
		case 1: storage[1].origin = storage[0].origin; break;
		case 2: storage[1].system_identifier++; break;
		case 3: storage[0].database_incarnation++; break;
		case 4: storage[0].state = CLUSTER_RELMAP_RETENTION_COMPLETE; break;
		case 5: storage[1].origin = CLUSTER_RELMAP_RETENTION_MAX_ORIGINS; break;
		}
		UT_ASSERT_NE(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
		UT_ASSERT(restore_refused(storage));
		if (kind != 0)
			UT_ASSERT_EQ(reads, 0);
	}
}

UT_TEST(test_restore_checks_original_native_floor_issued_and_half_space)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];

	for (int kind = 0; kind < 3; kind++) {
		setup(&identity);
		UT_ASSERT(stage(&identity));
		restore_builders(storage, directories);
		if (kind == 0)
			storage[1].native_floor = owner.top_full_xid + 1;
		else if (kind == 1)
			storage[1].last_issued = owner.top_full_xid - 1;
		else
			storage[1].last_issued = owner.top_full_xid + UINT64CONST(0x80000000);
		UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2),
			kind == 0 ? CLUSTER_RELMAP_RETENTION_NATIVE_GONE :
			kind == 1 ? CLUSTER_RELMAP_RETENTION_INVALID : CLUSTER_RELMAP_RETENTION_XID_WINDOW);
		UT_ASSERT(restore_refused(storage));
	}
}

UT_TEST(test_restore_failure_after_prior_namespace_retains_pins)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapV2Namespace other = identity;

	setup(&identity);
	UT_ASSERT(stage(&identity));
	restore_builders(storage, directories);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	other.shared = false;
	other.dbid = 42; /* Actual selected directory exists, but neither authority copy does. */
	UT_ASSERT_NE(cluster_relmap_retention_restore_namespace(&other, directories, 2), RESTORE_OK);
	UT_ASSERT(restore_refused(storage));
	UT_ASSERT_EQ(pin_count(&storage[1]), 1);
	UT_ASSERT_EQ(storage[1].pins[0].key.top_full_xid, owner.top_full_xid);
	UT_ASSERT_NE(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	UT_ASSERT_EQ(pin_count(&storage[1]), 1);
}

UT_TEST(test_restore_exact_retry_is_idempotent_but_changed_owner_conflicts)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapV2Image image = staged_image();
	unsigned char bytes[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	restore_builders(storage, directories);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	UT_ASSERT_EQ(pin_count(&storage[1]), 1);
	image.pending_owner.boot_incarnation++;
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2),
		CLUSTER_RELMAP_RETENTION_CONFLICT);
	UT_ASSERT(restore_refused(storage));
	UT_ASSERT_EQ(storage[1].pins[0].key.boot_incarnation, owner.boot_incarnation);
}

UT_TEST(test_restore_io_identity_and_close_failure_cannot_authorize_emptiness)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	char path[MAXPGPATH];

	for (int kind = 0; kind < 3; kind++) {
		setup(&identity);
		restore_builders(storage, directories);
		if (kind == 0) {
			snprintf(path, sizeof(path), "%s/%s", directory, names[1]);
			UT_ASSERT_EQ(unlink(path), 0);
			UT_ASSERT_EQ(symlink(names[0], path), 0);
		} else if (kind == 1)
			replace_during_read = true;
		else
			fail_directory_close = true;
		UT_ASSERT_NE(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
		UT_ASSERT(restore_refused(storage));
	}
}

UT_TEST(test_restore_capacity_failure_keeps_all_previously_restored_pins)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapRetentionKey key = { 0 };

	setup(&identity);
	UT_ASSERT(stage(&identity));
	restore_builders(storage, directories);
	key.identity = identity;
	key.identity.shared = false;
	key.generation = 8;
	key.origin = owner.node;
	key.top_full_xid = owner.top_full_xid;
	key.membership_epoch = 1;
	key.admitted_incarnation = owner.admitted_incarnation;
	key.boot_incarnation = owner.boot_incarnation;
	for (int i = 0; i < CLUSTER_RELMAP_RETENTION_CAPACITY; i++) {
		key.identity.dbid = 100 + i;
		UT_ASSERT_EQ(cluster_relmap_retention_build_add(&storage[1], &key, owner.wal_end), RESTORE_OK);
	}
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2),
		CLUSTER_RELMAP_RETENTION_CAPACITY_EXCEEDED);
	UT_ASSERT(restore_refused(storage));
	UT_ASSERT_EQ(pin_count(&storage[1]), CLUSTER_RELMAP_RETENTION_CAPACITY);
}

UT_TEST(test_restore_allows_distinct_global_and_database_pending)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapV2Image image = staged_image();
	unsigned char bytes[2176];

	setup(&identity);
	UT_ASSERT(stage(&identity));
	restore_builders(storage, directories);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories, 2), RESTORE_OK);
	image.identity.shared = false;
	image.identity.dbid = 42;
	snprintf(directory, sizeof(directory), "%s/base/42", root);
	UT_ASSERT_EQ(cluster_relmap_v2_encode(&image, bytes, sizeof(bytes)), CLUSTER_RELMAP_V2_VALID);
	write_copy(0, bytes, sizeof(bytes));
	write_copy(1, bytes, sizeof(bytes));
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&image.identity, directories, 2), RESTORE_OK);
	UT_ASSERT_EQ(pin_count(&storage[0]), 0);
	UT_ASSERT_EQ(pin_count(&storage[1]), 2);
	UT_ASSERT(storage[1].pins[0].key.identity.shared);
	UT_ASSERT(!storage[1].pins[1].key.identity.shared);
	UT_ASSERT_EQ(storage[1].pins[1].key.identity.dbid, 42);
	UT_ASSERT_EQ(storage[1].state, CLUSTER_RELMAP_RETENTION_BUILDING);
}

UT_TEST(test_restore_invalid_arguments_cannot_leave_build_publishable)
{
	ClusterRelmapRetentionDirectory storage[2], *directories[2];
	ClusterRelmapV2Namespace wrong;

	setup(&identity);
	for (int kind = 0; kind < 4; kind++) {
		restore_builders(storage, directories);
		wrong = identity;
		if (kind == 0)
			wrong.dbid = 42;
		else if (kind == 1)
			wrong.system_identifier = 0;
		else if (kind == 2)
			wrong.database_incarnation = 0;
		UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(kind == 3 ? NULL : &wrong,
			directories, 2), CLUSTER_RELMAP_RETENTION_INVALID);
		UT_ASSERT(restore_refused(storage));
	}
	restore_builders(storage, directories);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, NULL, 2), CLUSTER_RELMAP_RETENTION_INVALID);
	UT_ASSERT_EQ(cluster_relmap_retention_restore_namespace(&identity, directories,
		CLUSTER_RELMAP_RETENTION_MAX_ORIGINS + 1), CLUSTER_RELMAP_RETENTION_INVALID);
	UT_ASSERT_EQ(reads, 0);
}

UT_TEST(test_stage_requires_complete_existing_pin)
{
	for (int kind = 0; kind < 3; kind++) {
		setup(&identity);
		LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
		if (kind == 0)
			writer_retention.state = CLUSTER_RELMAP_RETENTION_BUILDING;
		else if (kind == 1)
			cluster_relmap_retention_invalidate(&writer_retention);
		else
			cancelled = cluster_relmap_retention_cancel_prepare(&writer_retention, &writer_key);
		LWLockRelease(&retention_lock);
		UT_ASSERT(!stage(&identity));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(renames, 0);
	}
}

UT_TEST(test_stage_owner_must_match_exact_pin)
{
	for (int field = 0; field < 8; field++) {
		ClusterRelmapV2Namespace ns = identity;
		ClusterRelmapV2Owner changed = owner;

		setup(&identity);
		switch (field) {
			case 0: changed.node++; break;
			case 1: changed.top_full_xid += UINT64CONST(0x100000000); break;
			case 2: changed.membership_epoch++; break;
			case 3: changed.admitted_incarnation++; break;
			case 4: changed.boot_incarnation++; break;
			case 5: ns.system_identifier++; break;
			case 6: ns.database_incarnation++; break;
			case 7: ns.shared = false; ns.dbid = 42; break;
		}
		UT_ASSERT(!stage_request(&ns, 8, &changed));
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT(writer_retention.pins[0].occupied);
		UT_ASSERT(!writer_retention.pins[0].may_be_staged);
	}
}

UT_TEST(test_stage_requires_nonheld_retention_lock)
{
	for (int kind = 0; kind < 3; kind++) {
		RelMapFile map = map_image(40001);
		bool result;

		setup(&identity);
		if (kind == 2)
			LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
		result = cluster_relmap_authority_v2_write_pending(&identity, 8, &owner, &map,
			kind == 0 ? NULL : &writer_retention, kind == 1 ? NULL : &retention_lock);
		if (kind == 2)
			LWLockRelease(&retention_lock);
		UT_ASSERT(!result);
		UT_ASSERT_EQ(writes, 0);
		UT_ASSERT_EQ(reads, 0);
		UT_ASSERT_EQ(io_under_retention_lock, 0);
	}
}

UT_TEST(test_pin_is_irrevocable_before_first_pending_write)
{
	setup(&identity);
	cancel_on_write = true;
	UT_ASSERT(stage(&identity));
	UT_ASSERT_EQ(cancelled, CLUSTER_RELMAP_RETENTION_RETAINED);
	UT_ASSERT_EQ(writes_without_pin, 0);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT(!retention_held);
	UT_ASSERT_EQ(retention_acquires, retention_releases);
	UT_ASSERT(retention_acquires >= 2);
	UT_ASSERT(writer_retention.pins[0].occupied);
	UT_ASSERT(writer_retention.pins[0].may_be_staged);
	UT_ASSERT_EQ(writer_retention.pins[0].wal_end, owner.wal_end);
}

UT_TEST(test_preflight_cancellation_prevents_pending_write)
{
	setup(&identity);
	cancel_on_read = true;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(cancelled, CLUSTER_RELMAP_RETENTION_OK);
	UT_ASSERT(!writer_retention.pins[0].occupied);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(renames, 0);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
	UT_ASSERT(!retention_held);
}

UT_TEST(test_preflight_invalidation_prevents_pending_write)
{
	setup(&identity);
	invalidate_on_read = true;
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writer_retention.state, CLUSTER_RELMAP_RETENTION_UNKNOWN);
	UT_ASSERT(writer_retention.pins[0].occupied);
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(renames, 0);
	UT_ASSERT_EQ(io_under_retention_lock, 0);
}

UT_TEST(test_changed_wal_cannot_rebind_staged_pin)
{
	setup(&identity);
	LWLockAcquire(&retention_lock, LW_EXCLUSIVE);
	UT_ASSERT_EQ(cluster_relmap_retention_stage_start(&writer_retention, &writer_key,
		owner.wal_end + 1), CLUSTER_RELMAP_RETENTION_OK);
	LWLockRelease(&retention_lock);
	UT_ASSERT(!stage(&identity));
	UT_ASSERT_EQ(writes, 0);
	UT_ASSERT_EQ(writer_retention.pins[0].wal_end, owner.wal_end + 1);
	UT_ASSERT(writer_retention.pins[0].occupied && writer_retention.pins[0].may_be_staged);
}

#include "test_cluster_relmap_runtime_storage.inc"
#include "test_cluster_relmap_owner_conversion.inc"

int
main(void)
{
	UT_PLAN(79);
	UT_RUN(test_owner_conversion_real_pair_preserves_building_directory);
	UT_RUN(test_owner_conversion_real_pending_refusal_preserves_both_copies);
	UT_RUN(test_owner_conversion_real_mixed_cut_requires_distinct_retry);
	UT_RUN(test_owner_conversion_real_final_owner_drift_is_not_rollback);
	UT_RUN(test_stage_both_copies_before_success);
	UT_RUN(test_committed_reader_hides_pending_and_rejects_degraded_pair);
	UT_RUN(test_committed_reader_requires_namespace_x_lock);
	UT_RUN(test_committed_reader_rechecks_namespace_x_lock_after_io);
	UT_RUN(test_committed_reader_rebuilds_exact_structural_pending);
	UT_RUN(test_committed_rebuild_accepts_local_default_and_custom_tablespaces);
	UT_RUN(test_committed_rebuild_rejects_foreign_physical_namespace_atomically);
	UT_RUN(test_per_database_namespace);
	UT_RUN(test_missing_authority_is_never_seeded);
	UT_RUN(test_fallback_is_incomplete_and_cannot_stage);
	UT_RUN(test_wrong_identity_format_or_owner_does_not_write);
	UT_RUN(test_existing_pending_is_never_overwritten);
	UT_RUN(test_symlink_copies_and_temp_are_rejected);
	UT_RUN(test_inode_replaced_during_read_is_not_accepted);
	UT_RUN(test_each_io_failure_retains_the_obligation);
	UT_RUN(test_corrupt_or_replaced_readback_never_acknowledges_stage);
	UT_RUN(test_v2_mutations_require_namespace_x_lock);
	UT_RUN(test_publish_and_discard_clear_both_copies);
	UT_RUN(test_partial_stage_can_be_discarded);
	UT_RUN(test_primary_done_keeps_backup_pending_until_distinct_retry);
	UT_RUN(test_exact_stage_identity_and_images_required_before_writes);
	UT_RUN(test_unknown_or_conflicting_copy_never_grants_cleanup);
	UT_RUN(test_each_finish_io_failure_is_retained_unknown);
	UT_RUN(test_already_clean_call_requires_new_sync_and_postread);
	UT_RUN(test_finish_rechecks_primary_after_backup_install);
	UT_RUN(test_directory_close_failure_cannot_acknowledge_clean_pair);
	UT_RUN(test_clean_unstaged_pair_cannot_fabricate_publish);
	UT_RUN(test_pending_reader_preserves_base_for_cold_finish);
	UT_RUN(test_conversion_syncs_both_and_disables_old_reader);
	UT_RUN(test_conversion_local_namespace_and_inactive_v1_bytes);
	UT_RUN(test_conversion_never_resolves_v1_pending);
	UT_RUN(test_conversion_validates_exact_v1_header_and_native_image);
	UT_RUN(test_conversion_rejects_conflicting_v1_copies);
	UT_RUN(test_conversion_backup_is_replaced_before_primary);
	UT_RUN(test_conversion_mixed_pair_requires_exact_clean_matching_state);
	UT_RUN(test_conversion_io_failures_require_a_distinct_successful_retry);
	UT_RUN(test_conversion_already_v2_retry_still_requires_sync);
	UT_RUN(test_conversion_refuses_missing_short_overlong_and_unsafe_files);
	UT_RUN(test_conversion_requires_identity_sync_policy_and_continuous_namespace_lock);
	UT_RUN(test_conversion_postread_corruption_replacement_and_close_cannot_acknowledge);
	UT_RUN(test_restore_clean_pair_is_only_one_building_observation);
	UT_RUN(test_restore_exact_original_origin_and_epoch_zero);
	UT_RUN(test_committed_rebuild_at_legal_stage_publish_discard_cuts);
	UT_RUN(test_committed_rebuild_rejects_valid_crc_pair_conflicts);
	UT_RUN(test_restore_incomplete_corrupt_foreign_and_conflicting_pairs_poison_build);
	UT_RUN(test_restore_requires_qualified_distinct_original_origin_builders);
	UT_RUN(test_restore_checks_original_native_floor_issued_and_half_space);
	UT_RUN(test_restore_failure_after_prior_namespace_retains_pins);
	UT_RUN(test_restore_exact_retry_is_idempotent_but_changed_owner_conflicts);
	UT_RUN(test_restore_io_identity_and_close_failure_cannot_authorize_emptiness);
	UT_RUN(test_restore_capacity_failure_keeps_all_previously_restored_pins);
	UT_RUN(test_restore_allows_distinct_global_and_database_pending);
	UT_RUN(test_restore_invalid_arguments_cannot_leave_build_publishable);
	UT_RUN(test_stage_requires_complete_existing_pin);
	UT_RUN(test_stage_owner_must_match_exact_pin);
	UT_RUN(test_stage_requires_nonheld_retention_lock);
	UT_RUN(test_pin_is_irrevocable_before_first_pending_write);
	UT_RUN(test_preflight_cancellation_prevents_pending_write);
	UT_RUN(test_preflight_invalidation_prevents_pending_write);
	UT_RUN(test_changed_wal_cannot_rebind_staged_pin);
	UT_RUN(test_runtime_storage_original_namespace_origin_and_committed_reader);
	UT_RUN(test_runtime_storage_first_write_failure_keeps_shared_pin);
	UT_RUN(test_runtime_storage_later_io_failures_keep_shared_pin);
	UT_RUN(test_runtime_storage_bad_preflight_allows_only_unstaged_cancel);
	UT_RUN(test_runtime_storage_unknown_building_never_reach_files);
	UT_RUN(test_runtime_storage_preflight_cancel_and_invalidate);
	UT_RUN(test_runtime_storage_final_native_drift_keeps_pending_and_pin);
	UT_RUN(test_runtime_storage_clear_pair_keeps_shared_pin);
	UT_RUN(test_runtime_storage_terminal_preserves_active_boundary_and_teardown_refusal);
	UT_RUN(test_runtime_storage_terminal_requires_exact_staged_pin);
	UT_RUN(test_runtime_storage_terminal_rechecks_original_image_and_owner);
	UT_RUN(test_runtime_storage_terminal_final_drift_retains_obligation);
	UT_RUN(test_runtime_storage_terminal_io_failure_and_explicit_retry);
	UT_RUN(test_runtime_storage_late_binding_enrolls_both_staged_namespaces);
	UT_RUN(test_runtime_storage_late_binding_precedes_second_namespace_begin);
	cleanup();
	free(runtime_storage_allocation);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
