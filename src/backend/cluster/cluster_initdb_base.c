/* Original standalone initdb owner of a new typed shared DATA base.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlog_internal.h"
#include "access/xloginsert.h"
#include "catalog/pg_tablespace_d.h"
#include "catalog/pg_control.h"
#include "catalog/storage_xlog.h"
#include "cluster/cluster_initdb_base.h"
#include "cluster/cluster_initdb_config.h"
#include "cluster/cluster_page_producer.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_space_reservation.h"
#include "cluster/cluster_wal_thread.h"
#include "common/file_perm.h"
#include "common/cryptohash.h"
#include "common/pgrac_initdb_wal.h"
#include "miscadmin.h"
#include "storage/bufpage.h"

/* Only initdb's small, fixed source tree is accepted; never size an allocation
 * from arbitrary relation contents or adopt an existing shared tree. */
#define BASE_MAX_DIRS 64
#define BASE_MAX_FILES 16384

typedef struct BaseDirectory
{
	int source;
	int target;
	Oid database;
	char name[16];
} BaseDirectory;

typedef struct BaseFile
{
	uint32 directory;
	RelFileNumber relation;
	ForkNumber fork;
	uint32 segment;
	BlockNumber blocks;
	char name[40];
	struct stat identity;
} BaseFile;

typedef struct BaseCreate
{
	const PgracInitdbWalContext *context;
	BaseDirectory dirs[BASE_MAX_DIRS];
	uint32 ndirs;
	BaseFile *files;
	uint32 nfiles;
} BaseCreate;

static void pg_attribute_noreturn()
base_refuse(const char *reason)
{
	ereport(FATAL, (errmsg("INITDB_BASE_CREATE: %s", reason)));
	pg_unreachable();
}

static bool
same_inode(const struct stat *a, const struct stat *b)
{
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino
		&& a->st_size == b->st_size && a->st_mode == b->st_mode
		&& a->st_nlink == b->st_nlink && a->st_uid == b->st_uid
		&& a->st_mtime == b->st_mtime && a->st_ctime == b->st_ctime;
}

static int
open_directory(int parent, const char *name)
{
	struct stat st;
	int fd = openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);

	if (fd < 0 || fstat(fd, &st) != 0 || !S_ISDIR(st.st_mode)
		|| st.st_uid != geteuid() || (st.st_mode & 0022) != 0)
		base_refuse("directory identity or ownership is invalid");
	return fd;
}

static DIR *
directory_stream(int fd)
{
	/* An independent open description prevents enumeration from changing
	 * the retained descriptor's directory offset. */
	int copy = open_directory(fd, ".");
	DIR *dir = fdopendir(copy);
	if (dir == NULL)
		base_refuse("cannot enumerate original directory");
	return dir;
}

static void
require_empty(int fd)
{
	DIR *dir = directory_stream(fd);
	struct dirent *entry;

	errno = 0;
	while ((entry = readdir(dir)) != NULL)
		if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0)
			base_refuse("shared target is not empty");
	if (errno != 0 || closedir(dir) != 0)
		base_refuse("cannot finish target enumeration");
}

static uint32
number(const char *text, char **end)
{
	unsigned long value;

	if (text[0] < '1' || text[0] > '9')
		base_refuse("noncanonical numeric source name");
	errno = 0;
	value = strtoul(text, end, 10);
	if (errno != 0 || value > UINT32_MAX)
		base_refuse("numeric source name is out of range");
	return (uint32) value;
}

static void
file_name(const char *name, BaseFile *file)
{
	char *end;

	file->relation = number(name, &end);
	file->fork = MAIN_FORKNUM;
	if (strncmp(end, "_fsm", 4) == 0)
	{
		file->fork = FSM_FORKNUM;
		end += 4;
	}
	else if (strncmp(end, "_vm", 3) == 0)
	{
		file->fork = VISIBILITYMAP_FORKNUM;
		end += 3;
	}
	if (*end == '.')
		file->segment = number(end + 1, &end);
	if (*end != '\0' || strlen(name) >= sizeof(file->name)
		|| (uint64) file->segment * RELSEG_SIZE >= InvalidBlockNumber)
		base_refuse("unsupported relation source filename");
	strlcpy(file->name, name, sizeof(file->name));
}

static int
source_open(BaseCreate *create, const BaseFile *file)
{
	int parent = create->dirs[file->directory].source;
	struct stat held, named;
	int fd = openat(parent, file->name, O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);

	if (fd < 0 || fstat(fd, &held) != 0
		|| fstatat(parent, file->name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_inode(&held, &named) || !same_inode(&held, &file->identity))
		base_refuse("original source file changed");
	return fd;
}

static void
source_read(const BaseFile *file, int fd, BlockNumber index, PGAlignedBlock *page)
{
	ssize_t n;
	BlockNumber block = (uint64) file->segment * RELSEG_SIZE + index;

	do { n = pread(fd, page->data, BLCKSZ, (off_t) index * BLCKSZ); }
	while (n < 0 && errno == EINTR);
	if (n != BLCKSZ || !PageIsVerifiedExtended(page->data, block, 0)
		|| (((PageHeader) page->data)->pd_flags & (PD_SPACE_METADATA | PD_UNDO_SEG_HEADER)) != 0
		|| (file->fork != FSM_FORKNUM && (PageIsNew(page->data)
			|| ((PageHeader) page->data)->pd_block_scn != 0)))
		base_refuse("original source page is not a complete native initialization page");
}

static void
source_close(BaseCreate *create, const BaseFile *file, int fd)
{
	struct stat held, named;

	if (fstat(fd, &held) != 0
		|| fstatat(create->dirs[file->directory].source, file->name,
			&named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_inode(&held, &named) || !same_inode(&held, &file->identity)
		|| close(fd) != 0)
		base_refuse("original source changed during readback");
}

static void
collect_directory(BaseCreate *create, int fd, Oid database, const char *name)
{
	BaseDirectory *directory;
	DIR *dir;
	struct dirent *entry;
	uint32 index = create->ndirs++;

	if (index >= BASE_MAX_DIRS)
		base_refuse("too many initialization databases");
	directory = &create->dirs[index];
	directory->source = fd;
	directory->database = database;
	strlcpy(directory->name, name, sizeof(directory->name));
	dir = directory_stream(fd);
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		BaseFile *file;
		int source;
		PGAlignedBlock page;

		if (entry->d_name[0] < '0' || entry->d_name[0] > '9')
			continue;
		if (create->nfiles == BASE_MAX_FILES)
			base_refuse("too many initialization relation files");
		file = &create->files[create->nfiles++];
		file->directory = index;
		file_name(entry->d_name, file);
		if (fstatat(fd, file->name, &file->identity, AT_SYMLINK_NOFOLLOW) != 0
			|| !S_ISREG(file->identity.st_mode) || file->identity.st_uid != geteuid()
			|| file->identity.st_nlink != 1 || (file->identity.st_mode & 0022) != 0
			|| file->identity.st_size < 0 || file->identity.st_size % BLCKSZ != 0
			|| file->identity.st_size / BLCKSZ > RELSEG_SIZE)
			base_refuse("unsafe or partial initialization relation file");
		file->blocks = file->identity.st_size / BLCKSZ;
		source = source_open(create, file);
		for (BlockNumber block = 0; block < file->blocks; ++block)
			source_read(file, source, block, &page);
		source_close(create, file, source);
		errno = 0;
	}
	if (errno != 0 || closedir(dir) != 0)
		base_refuse("cannot finish original source enumeration");
}

static int
file_order(const void *left, const void *right)
{
	const BaseFile *a = left, *b = right;
#define CMP_FIELD(field) if (a->field != b->field) return a->field < b->field ? -1 : 1
	CMP_FIELD(directory);
	CMP_FIELD(relation);
	CMP_FIELD(fork);
	CMP_FIELD(segment);
#undef CMP_FIELD
	return 0;
}

static uint32
relation_end(BaseCreate *create, uint32 start, BlockNumber *blocks)
{
	const BaseFile *first = &create->files[start];
	uint32 end = start;
	uint64 main_blocks = 0;

	if (first->fork != MAIN_FORKNUM || first->segment != 0)
		base_refuse("relation has no original MAIN file");
	while (end < create->nfiles)
	{
		const BaseFile *file = &create->files[end];
		if (file->directory != first->directory || file->relation != first->relation)
			break;
		if (end == start || file->fork != create->files[end - 1].fork)
		{
			if (file->segment != 0)
				base_refuse("fork has no first segment");
		}
		else if (file->segment != create->files[end - 1].segment + 1
			|| create->files[end - 1].blocks != RELSEG_SIZE)
			base_refuse("relation source segment gap or short predecessor");
		if (file->fork == MAIN_FORKNUM)
			main_blocks += file->blocks;
		++end;
	}
	if (main_blocks >= InvalidBlockNumber)
		base_refuse("initialization relation exceeds the block address space");
	*blocks = (BlockNumber) main_blocks;
	return end;
}

static void
write_page(int fd, BlockNumber block, Page page)
{
	ssize_t n;
	size_t done = 0;

	PageSetChecksumInplace(page, block);
	while (done < BLCKSZ)
	{
		n = pwrite(fd, page + done, BLCKSZ - done, (off_t) block * BLCKSZ + done);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			base_refuse("could not write original target page");
		done += n;
	}
}

static void
stamp(Page page, XLogRecPtr lsn)
{
	if (!PageIsNew(page))
	{
		PageSetLSNPreserveOrigin(page, lsn);
		if (!PageSetLSNOrigin(page, 0))
			base_refuse("invalid founder page origin");
	}
}

static pg_cryptohash_ctx *
new_digest(void)
{
	pg_cryptohash_ctx *digest = pg_cryptohash_create(PG_SHA256);
	if (digest == NULL || pg_cryptohash_init(digest) < 0)
		base_refuse("cannot prepare original write readback");
	return digest;
}

static void
sync_readback_close(int parent, const char *name, int fd, BlockNumber blocks,
	pg_cryptohash_ctx *written)
{
	pg_cryptohash_ctx *actual = new_digest();
	uint8 expected_hash[32], actual_hash[32];
	PGAlignedBlock page;
	struct stat held, named;

	if (fsync(fd) != 0)
		base_refuse("cannot persist original target file");
	for (BlockNumber i = 0; i < blocks; ++i)
	{
		ssize_t n;
		do { n = pread(fd, page.data, BLCKSZ, (off_t) i * BLCKSZ); }
		while (n < 0 && errno == EINTR);
		if (n != BLCKSZ || pg_cryptohash_update(actual, (uint8 *) page.data, BLCKSZ) < 0)
			base_refuse("cannot read persisted original target");
	}
	if (pg_cryptohash_final(written, expected_hash, sizeof(expected_hash)) < 0
		|| pg_cryptohash_final(actual, actual_hash, sizeof(actual_hash)) < 0
		|| memcmp(expected_hash, actual_hash, sizeof(expected_hash)) != 0
		|| fstat(fd, &held) != 0 || fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_inode(&held, &named) || !S_ISREG(held.st_mode)
		|| held.st_uid != geteuid() || held.st_nlink != 1
		|| held.st_size != (off_t) blocks * BLCKSZ || (held.st_mode & 0022) != 0
		|| close(fd) != 0)
		base_refuse("persisted original target bytes or identity changed");
	pg_cryptohash_free(actual);
	pg_cryptohash_free(written);
}

static void
sync_directory_close(int parent, const char *name, int fd)
{
	struct stat held, named;

	if (fsync(fd) != 0 || fstat(fd, &held) != 0
		|| fstatat(parent, name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| !same_inode(&held, &named) || !S_ISDIR(held.st_mode)
		|| held.st_uid != geteuid() || (held.st_mode & 0022) != 0 || close(fd) != 0)
		base_refuse("persisted original directory identity changed");
}

static void
create_space(BaseCreate *create, uint32 start, BlockNumber blocks,
	ClusterSpaceIdentity *identity)
{
	BaseFile *first = &create->files[start];
	ClusterSpaceStructureChange structure = {0};
	ClusterSpaceReservationChange advance = {0};
	PGAlignedBlock pages[2] = {0};
	uint8 bytes[CLUSTER_SPACE_STRUCTURE_WAL_BYTES];
	uint8 apply;
	char name[40];
	XLogRecPtr lsn;
	int fd;
	pg_cryptohash_ctx *written;

	structure.identity.action = CLUSTER_SPACE_WAL_CREATE;
	structure.identity.nblocks = InvalidBlockNumber;
	structure.identity.result_token = rf_page_mutation_token_next();
	identity->key.system_identifier = create->context->system_identifier;
	identity->key.database_incarnation = create->context->database_incarnation;
	memcpy(identity->key.storage_uuid, create->context->storage_uuid, 16);
	identity->key.locator.spcOid = first->directory == 0 ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
	identity->key.locator.dbOid = create->dirs[first->directory].database;
	identity->key.locator.relNumber = first->relation;
	identity->sequence = 1;
	identity->operation = structure.identity.result_token;
	identity->state = CLUSTER_SPACE_IDENTITY_LIVE;
	if (!pg_strong_random(identity->incarnation, sizeof(identity->incarnation)))
		base_refuse("cannot generate original relation identity");
	structure.identity.result = *identity;
	structure.reservation.action = CLUSTER_SPACE_RESERVATION_INIT;
	structure.reservation.result.identity = *identity;
	structure.reservation.result_token = structure.identity.result_token;
	if (!cluster_space_structure_wal_encode(&structure, bytes, sizeof(bytes))
		|| cluster_space_structure_apply(&structure, &identity->key,
			pages[0].data, pages[1].data, BLCKSZ, &apply) != CLUSTER_SPACE_IDENTITY_APPLY || apply != 3)
		base_refuse("cannot prepare original SPACE creation");
	snprintf(name, sizeof(name), "%u_space", first->relation);
	fd = openat(create->dirs[first->directory].target, name,
		O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, pg_file_create_mode);
	if (fd < 0)
		base_refuse("SPACE target already exists or cannot be created");
	XLogBeginInsert();
	XLogRegisterData((char *) bytes, sizeof(bytes));
	lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_SPACE_IDENTITY | XLR_SPECIAL_REL_UPDATE);
	XLogFlush(lsn);
	for (int i = 0; i < 2; ++i)
	{
		stamp(pages[i].data, lsn);
		write_page(fd, i, pages[i].data);
	}
	if (blocks != 0)
	{
		advance.action = CLUSTER_SPACE_RESERVATION_ADVANCE;
		advance.before = advance.result = structure.reservation.result;
		advance.before_token = structure.reservation.result_token;
		advance.result_token = rf_page_mutation_token_next();
		advance.granted = advance.result.next_block = blocks;
		if (!cluster_space_reservation_wal_encode(&advance, bytes, CLUSTER_SPACE_RESERVATION_WAL_BYTES)
			|| cluster_space_reservation_apply(&advance, &identity->key, pages[1].data, BLCKSZ)
				!= CLUSTER_SPACE_IDENTITY_APPLY)
			base_refuse("cannot prepare original SPACE reservation");
		XLogBeginInsert();
		XLogRegisterData((char *) bytes, CLUSTER_SPACE_RESERVATION_WAL_BYTES);
		lsn = XLogInsert(RM_SMGR_ID, XLOG_SMGR_SPACE_RESERVATION | XLR_SPECIAL_REL_UPDATE);
		XLogFlush(lsn);
		stamp(pages[1].data, lsn);
		write_page(fd, 1, pages[1].data);
	}
	written = new_digest();
	if (pg_cryptohash_update(written, (uint8 *) pages, sizeof(pages)) < 0)
		base_refuse("cannot bind original SPACE bytes");
	sync_readback_close(create->dirs[first->directory].target, name, fd, 2, written);
}

static void
copy_file(BaseCreate *create, const BaseFile *file, const ClusterSpaceIdentity *identity)
{
	RelFileLocator locator = identity->key.locator;
	int source = source_open(create, file);
	int target = openat(create->dirs[file->directory].target, file->name,
		O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, pg_file_create_mode);
	pg_cryptohash_ctx *written = new_digest();

	if (target < 0)
		base_refuse("DATA target already exists or cannot be created");
	for (BlockNumber i = 0; i < file->blocks; ++i)
	{
		PGAlignedBlock before = {0}, page;
		RfPageProducerComponentV1 component = {0};
		RfPageProducerBatchV1 batch;
		BlockNumber block = (uint64) file->segment * RELSEG_SIZE + i;
		XLogRecPtr lsn;

		source_read(file, source, i, &page);
		if (file->fork == FSM_FORKNUM)
		{
			component.page_class = RF_PAGE_CLASS_REBUILDABLE_FSM;
			component.before_kind = RF_PAGE_STATE_REBUILDABLE;
		}
		else
		{
			component.page_class = RF_PAGE_CLASS_ORDINARY;
			component.before_kind = RF_PAGE_STATE_ABSENT;
			component.page = before.data;
			memcpy(component.segment_incarnation, identity->incarnation, 16);
		}
		if (!rf_page_producer_prepare_v1(&component, 1, &batch))
			base_refuse("cannot prepare original DATA version");
		memcpy(before.data, page.data, BLCKSZ);
		if (!rf_page_producer_stamp_v1(&batch))
			base_refuse("original DATA version changed before WAL");
		XLogBeginInsert();
		XLogRegisterBlock(0, &locator, file->fork, block, before.data, REGBUF_FORCE_IMAGE);
		if (!rf_page_producer_register_wal_v1(&batch))
			base_refuse("cannot register original DATA version");
		lsn = XLogInsert(RM_XLOG_ID, XLOG_FPI);
		XLogFlush(lsn);
		stamp(before.data, lsn);
		/* Checksum uses the logical block, while this descriptor is one
		 * relation segment.  Do not confuse the two offsets. */
		PageSetChecksumInplace(before.data, block);
		if (pg_cryptohash_update(written, (uint8 *) before.data, BLCKSZ) < 0)
			base_refuse("cannot bind original DATA bytes");
		{
			ssize_t n;
			do { n = pwrite(target, before.data, BLCKSZ, (off_t) i * BLCKSZ); }
			while (n < 0 && errno == EINTR);
			if (n != BLCKSZ)
				base_refuse("could not write complete original DATA page");
		}
	}
	source_close(create, file, source);
	sync_readback_close(create->dirs[file->directory].target, file->name, target,
		file->blocks, written);
}

void
cluster_initdb_base_create(int exit_code)
{
	const PgracInitdbWalContext *context = cluster_wal_thread_initdb_context();
	BaseCreate *create;
	ClusterInitdbConfig *config;
	struct stat data_st, target_st;
	int source, base, target_base;
	DIR *dir;
	struct dirent *entry;

	if (context == NULL || context->base_fd == 0)
		return;
	if (exit_code != 0 || IsUnderPostmaster || IsTransactionState()
		|| context->phase != PGRAC_INITDB_WAL_POSTBOOTSTRAP || context->thread_id != 1
		|| context->base_fd < 3 || !DataChecksumsEnabled() || RecoveryInProgress())
		base_refuse("only the successful original founder can create shared DATA");
	source = open_directory(AT_FDCWD, ".");
	if (fstat(source, &data_st) != 0 || fstat(context->base_fd, &target_st) != 0
		|| (uint64) data_st.st_dev != context->data_device || (uint64) data_st.st_ino != context->data_inode
		|| (uint64) target_st.st_dev != context->base_device || (uint64) target_st.st_ino != context->base_inode)
		base_refuse("original source or target identity changed");
	require_empty(context->base_fd);
	config = cluster_initdb_config_prepare(context);
	/* Own the actual native flush before reading the completed SQL/catalog
	 * tree. The later shutdown checkpoint covers the typed copy records. */
	CreateCheckPoint(CHECKPOINT_IMMEDIATE | CHECKPOINT_FORCE);
	create = palloc0(sizeof(*create));
	create->context = context;
	create->files = palloc0(sizeof(BaseFile) * BASE_MAX_FILES);
	collect_directory(create, open_directory(source, "global"), InvalidOid, "global");
	base = open_directory(source, "base");
	dir = directory_stream(base);
	errno = 0;
	while ((entry = readdir(dir)) != NULL)
	{
		char *end;
		Oid database;
		if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
			continue;
		database = number(entry->d_name, &end);
		if (*end != '\0')
			base_refuse("unsupported original database directory");
		collect_directory(create, open_directory(base, entry->d_name), database, entry->d_name);
		errno = 0;
	}
	if (errno != 0 || closedir(dir) != 0 || close(base) != 0 || close(source) != 0)
		base_refuse("cannot finish original database census");
	if (create->nfiles == 0 || create->ndirs < 2)
		base_refuse("original database has no catalog base");
	qsort(create->files, create->nfiles, sizeof(BaseFile), file_order);
	for (uint32 i = 0; i < create->nfiles;)
	{
		BlockNumber blocks;
		i = relation_end(create, i, &blocks);
	}
	if (!cluster_scn_initdb_base_begin())
		base_refuse("original founder has no unused SCN allocator");
	require_empty(context->base_fd);
	if (mkdirat(context->base_fd, "global", pg_dir_create_mode) != 0
		|| mkdirat(context->base_fd, "base", pg_dir_create_mode) != 0)
		base_refuse("cannot exclusively create shared base directories");
	create->dirs[0].target = open_directory(context->base_fd, "global");
	target_base = open_directory(context->base_fd, "base");
	for (uint32 i = 1; i < create->ndirs; ++i)
	{
		if (mkdirat(target_base, create->dirs[i].name, pg_dir_create_mode) != 0)
			base_refuse("cannot exclusively create shared database directory");
		create->dirs[i].target = open_directory(target_base, create->dirs[i].name);
	}
	for (uint32 i = 0; i < create->nfiles;)
	{
		BlockNumber blocks;
		ClusterSpaceIdentity identity = {0};
		uint32 end = relation_end(create, i, &blocks);
		create_space(create, i, blocks, &identity);
		for (; i < end; ++i)
			copy_file(create, &create->files[i], &identity);
	}
	cluster_initdb_config_create(config, create->dirs[0].target);
	for (uint32 i = 0; i < create->ndirs; ++i)
	{
		sync_directory_close(i == 0 ? context->base_fd : target_base,
			create->dirs[i].name, create->dirs[i].target);
		if (close(create->dirs[i].source) != 0)
			base_refuse("cannot close original source directory");
	}
	sync_directory_close(context->base_fd, "base", target_base);
	if (fsync(context->base_fd) != 0)
		base_refuse("cannot persist original shared base root");
	pfree(create->files);
	pfree(create);
}
