/* Same-build original-creator delivery. No durable format or ROOT authority. */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif
#include "common/pgrac_initdb_recovery.h"
#ifdef WIN32
bool
pgrac_initdb_recovery_prepare_files_v2(const ClusterRecoveryFileIdentityV2 *a,
									   const ClusterRecoveryFileIdentityV2 b[2],
									   const PgracInitdbConfigContext *c, int d, int e,
									   PgracInitdbRecoveryContext *out)
{
	return false;
}
bool
pgrac_initdb_recovery_boot_v2(const PgracInitdbRecoveryContext *c,
							  ClusterRecoveryFileIdentityV2 out[2])
{
	return false;
}
bool
pgrac_initdb_recovery_relay_v2(const PgracInitdbRecoveryContext *c, int *out)
{
	return false;
}

bool
pgrac_initdb_recovery_prepare_v2(const ClusterRecoveryFileIdentityV2 *id,
								 const PgracInitdbConfigContext *c, int a, int b,
								 PgracInitdbRecoveryContext *out)
{
	return false;
}
bool
pgrac_initdb_recovery_relmaps_v2(const PgracInitdbRecoveryContext *c,
								 ClusterRecoveryFileIdentityV2 out[4])
{
	return false;
}
bool
pgrac_initdb_recovery_pipe_v2(const PgracInitdbRecoveryContext *c, int *out)
{
	return false;
}
bool
pgrac_initdb_recovery_read_v2(int fd, const PgracInitdbConfigContext *c, uint64 s, uint64 d,
							  const uint8 u[16], int a, int b, PgracInitdbRecoveryContext *out)
{
	return false;
}
bool
pgrac_initdb_recovery_relmap_observe_v2(const PgracInitdbRecoveryContext *ctx, int data_fd,
											 int shared_fd,
											 ClusterRecoveryCheckpointFileV2 out[8]) {
	return false;
}
bool
pgrac_initdb_recovery_relmap_current_v2(
	const PgracInitdbRecoveryContext *ctx, int data_fd, int shared_fd,
	const ClusterRecoveryCheckpointFileV2 captured[8]) {
	return false;
}

#else
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/pg_database_d.h"
#include "common/cluster_relmap_file.h"
#include "sha2_int.h"
#include "catalog/pg_tablespace_d.h"
#include "cluster/cluster_recovery_format.h"
#include "common/pgrac_initdb_recovery.h"

StaticAssertDecl(sizeof(PgracInitdbRecoveryContext) <= 512,
				 "closed request must fit minimum POSIX pipe capacity");

static bool
overlaps(const void *a, size_t an, const void *b, size_t bn)
{
	uintptr_t x = (uintptr_t)a, y = (uintptr_t)b;
	return x <= y ? y - x < an : x - y < bn;
}

static bool
directory(int fd, struct stat *st)
{
	return fstat(fd, st) == 0 && S_ISDIR(st->st_mode) && st->st_uid == geteuid()
		   && (st->st_mode & 0022) == 0;
}

static bool
config_valid(const PgracInitdbConfigContext *c)
{
	uint8 any = 0;
	if (!c || c->fd < 3 || c->bytes == 0 || c->bytes > PGRAC_INITDB_CONFIG_MAX_BYTES)
		return false;
	for (unsigned i = 0; i < sizeof(c->sha256); ++i)
		any |= c->sha256[i];
	return any != 0;
}

static bool
same_config(const PgracInitdbConfigContext *a, const PgracInitdbConfigContext *b)
{
	/* The child opens the SAME original configuration, at its own FD number. */
	return config_valid(a) && config_valid(b) && a->device == b->device && a->inode == b->inode
		   && a->bytes == b->bytes && memcmp(a->sha256, b->sha256, sizeof(a->sha256)) == 0;
}

bool
pgrac_initdb_recovery_relmaps_v2(const PgracInitdbRecoveryContext *ctx,
								 ClusterRecoveryFileIdentityV2 out[4])
{
	static const Oid databases[4] = { InvalidOid, Template1DbOid, Template0DbOid, PostgresDbOid };
	ClusterRecoveryFileIdentityV2 ids[4];
	uint8 encoded[128];
	if (!ctx || !out || overlaps(ctx, sizeof(*ctx), out, sizeof(ids))
		|| ctx->magic != PGRAC_INITDB_RECOVERY_MAGIC
		|| ctx->version != CLUSTER_RECOVERY_FORMAT_VERSION_V2 || ctx->creator_pid == 0
		|| !config_valid(&ctx->config) || ctx->first.kind != CLUSTER_RECOVERY_FILE_RELMAP_V2
		|| ctx->first.origin != 0 || ctx->first.redo_timeline != 1
		|| ctx->first.database_oid != InvalidOid
		|| ctx->first.tablespace_oid != GLOBALTABLESPACE_OID || ctx->first.object_id != 0
		|| ctx->first.creation_generation == 0 || ctx->first.creation_generation > UINT64_MAX - 4)
		return false;
	for (unsigned i = 0; i < 4; ++i) {
		ids[i] = ctx->first;
		ids[i].database_oid = databases[i];
		ids[i].tablespace_oid = i == 0 ? GLOBALTABLESPACE_OID : DEFAULTTABLESPACE_OID;
		ids[i].object_id = databases[i];
		ids[i].creation_generation += i;
		if (!cluster_recovery_file_identity_encode_v2(&ids[i], encoded, sizeof(encoded)))
			return false;
	}
	memcpy(out, ids, sizeof(ids));
	return true;
}

bool
pgrac_initdb_recovery_prepare_v2(const ClusterRecoveryFileIdentityV2 *first,
								 const PgracInitdbConfigContext *config, int data_fd, int shared_fd,
								 PgracInitdbRecoveryContext *out)
{
	PgracInitdbRecoveryContext ctx = { 0 };
	ClusterRecoveryFileIdentityV2 ids[4];
	struct stat data, shared;
	if (!first || !config || !out || overlaps(out, sizeof(*out), first, sizeof(*first))
		|| overlaps(out, sizeof(*out), config, sizeof(*config)))
		return false;
	ctx.magic = PGRAC_INITDB_RECOVERY_MAGIC;
	ctx.version = CLUSTER_RECOVERY_FORMAT_VERSION_V2;
	ctx.creator_pid = getpid();
	ctx.config = *config;
	ctx.first = *first;
	if (!pgrac_initdb_recovery_relmaps_v2(&ctx, ids) || !directory(data_fd, &data)
		|| !directory(shared_fd, &shared)
		|| (data.st_dev == shared.st_dev && data.st_ino == shared.st_ino))
		return false;
	ctx.data_device = data.st_dev;
	ctx.data_inode = data.st_ino;
	ctx.shared_device = shared.st_dev;
	ctx.shared_inode = shared.st_ino;
	*out = ctx;
	return true;
}

bool
pgrac_initdb_recovery_boot_v2(const PgracInitdbRecoveryContext *ctx,
							  ClusterRecoveryFileIdentityV2 out[2])
{
	ClusterRecoveryFileIdentityV2 relmaps[4], boot[2], expected;
	uint8 encoded[128];
	if (!ctx || !out || overlaps(ctx, sizeof(*ctx), out, sizeof(boot))
		|| ctx->boot_file_count != PGRAC_INITDB_BOOT_FILES_V2
		|| !pgrac_initdb_recovery_relmaps_v2(ctx, relmaps))
		return false;
	for (unsigned i = 0; i < 2; ++i) {
		boot[i] = ctx->boot_files[i];
		expected = ctx->first;
		expected.kind = i ? CLUSTER_RECOVERY_FILE_UNDO_V2 : CLUSTER_RECOVERY_FILE_CLOG_V2;
		expected.database_oid = expected.tablespace_oid = 0;
		expected.object_id = 0;
		/* Separate creator-allocated identities, beyond the entire relmap set.
		 * The page's actual FullXid birth remains the original producer's input. */
		expected.creation_generation = boot[i].creation_generation;
		if (boot[i].creation_generation <= relmaps[3].creation_generation
			|| (i && boot[i].creation_generation == boot[0].creation_generation)
			|| !cluster_recovery_file_identity_encode_v2(&expected, encoded, sizeof(encoded))
			|| !cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded), &boot[i]))
			return false;
	}
	memcpy(out, boot, sizeof(boot));
	return true;
}

static bool
request_files_valid(const PgracInitdbRecoveryContext *ctx)
{
	static const ClusterRecoveryFileIdentityV2 empty[2] = { { 0 } };
	ClusterRecoveryFileIdentityV2 boot[2];
	return ctx->boot_file_count == 0 ? memcmp(ctx->boot_files, empty, sizeof(empty)) == 0
									 : pgrac_initdb_recovery_boot_v2(ctx, boot);
}

bool
pgrac_initdb_recovery_prepare_files_v2(const ClusterRecoveryFileIdentityV2 *first,
									   const ClusterRecoveryFileIdentityV2 boot[2],
									   const PgracInitdbConfigContext *config, int data_fd,
									   int shared_fd, PgracInitdbRecoveryContext *out)
{
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryFileIdentityV2 checked[2];
	if (!first || !boot || !config || !out || overlaps(out, sizeof(*out), first, sizeof(*first))
		|| overlaps(out, sizeof(*out), boot, sizeof(ctx.boot_files))
		|| overlaps(out, sizeof(*out), config, sizeof(*config))
		|| !pgrac_initdb_recovery_prepare_v2(first, config, data_fd, shared_fd, &ctx))
		return false;
	ctx.boot_file_count = 2;
	memcpy(ctx.boot_files, boot, sizeof(ctx.boot_files));
	if (!pgrac_initdb_recovery_boot_v2(&ctx, checked))
		return false;
	*out = ctx;
	return true;
}

/* Forward only the already authenticated original request, through the same
 * closed-writer mechanism. The immediate native child's parent is initdb. */
bool
pgrac_initdb_recovery_relay_v2(const PgracInitdbRecoveryContext *ctx, int *out)
{
	PgracInitdbRecoveryContext relay;
	ClusterRecoveryFileIdentityV2 boot[2];
	if (!ctx || !out || overlaps(ctx, sizeof(*ctx), out, sizeof(*out))
		|| ctx->creator_pid != (uint64)getppid() || !pgrac_initdb_recovery_boot_v2(ctx, boot))
		return false;
	relay = *ctx;
	relay.creator_pid = getpid();
	return pgrac_initdb_recovery_pipe_v2(&relay, out);
}

bool
pgrac_initdb_recovery_pipe_v2(const PgracInitdbRecoveryContext *ctx, int *out)
{
	ClusterRecoveryFileIdentityV2 ids[4];
	int fds[2];
	ssize_t n;
	bool ok;
	if (!out || !ctx || overlaps(out, sizeof(*out), ctx, sizeof(*ctx))
		|| !pgrac_initdb_recovery_relmaps_v2(ctx, ids) || !request_files_valid(ctx)
		|| pipe(fds) != 0)
		return false;
	do {
		n = write(fds[1], ctx, sizeof(*ctx));
	} while (n < 0 && errno == EINTR);
	ok = n == sizeof(*ctx);
	if (close(fds[1]) != 0)
		ok = false;
	if (fds[0] < 3) {
		int replacement = fcntl(fds[0], F_DUPFD, 3);
		if (close(fds[0]) != 0 || replacement < 0) {
			if (replacement >= 0)
				(void)close(replacement);
			return false;
		}
		fds[0] = replacement;
	}
	if (fcntl(fds[0], F_SETFD, FD_CLOEXEC) != 0)
		ok = false;
	if (!ok) {
		(void)close(fds[0]);
		return false;
	}
	*out = fds[0];
	return true;
}

bool
pgrac_initdb_recovery_read_v2(int fd, const PgracInitdbConfigContext *config, uint64 system,
							  uint64 incarnation, const uint8 storage[16], int data_fd,
							  int shared_fd, PgracInitdbRecoveryContext *out)
{
	PgracInitdbRecoveryContext ctx;
	ClusterRecoveryFileIdentityV2 ids[4];
	struct stat pipe_st, data, shared;
	size_t used = 0;
	unsigned waits = 0;
	int flags;
	bool ok = false;
	char extra;

	/* Only a valid incoming request pipe is consumed. Never close an aliased
	 * original directory/config descriptor passed by mistake. */
	if (fd < 3 || fd == data_fd || fd == shared_fd || (config && fd == config->fd)
		|| fstat(fd, &pipe_st) != 0 || !S_ISFIFO(pipe_st.st_mode) || pipe_st.st_uid != geteuid()
		|| (flags = fcntl(fd, F_GETFL)) < 0 || (flags & O_ACCMODE) != O_RDONLY)
		return false;
	if (!config || !storage || !out || overlaps(out, sizeof(*out), config, sizeof(*config))
		|| overlaps(out, sizeof(*out), storage, 16) || fcntl(fd, F_SETFD, FD_CLOEXEC) != 0
		|| fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
		goto done;
	/* Same bounded closed-writer delivery policy as the original creation
	 * request. A live writer cannot turn partial bytes into a valid request. */
	for (;;) {
		ssize_t n = read(fd, used < sizeof(ctx) ? (char *)&ctx + used : &extra,
						 used < sizeof(ctx) ? sizeof(ctx) - used : 1);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0 && errno == EAGAIN) {
			struct pollfd event = { fd, POLLIN, 0 };
			if (++waits > 100 || poll(&event, 1, 100) < 0)
				goto done;
			continue;
		}
		if (n < 0 || (n > 0 && used == sizeof(ctx)))
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	if (used != sizeof(ctx) || ctx.creator_pid != (uint64)getppid()
		|| !same_config(&ctx.config, config) || ctx.first.system_identifier != system
		|| ctx.first.database_incarnation != incarnation
		|| memcmp(ctx.first.storage_uuid, storage, 16) != 0
		|| !pgrac_initdb_recovery_relmaps_v2(&ctx, ids) || !request_files_valid(&ctx)
		|| !directory(data_fd, &data) || !directory(shared_fd, &shared)
		|| ctx.data_device != data.st_dev || ctx.data_inode != data.st_ino
		|| ctx.shared_device != shared.st_dev || ctx.shared_inode != shared.st_ino
		|| ctx.creator_pid != (uint64)getppid())
		goto done;
	ok = true;
done:
	if (close(fd) != 0)
		ok = false;
	if (ok)
		*out = ctx;
	return ok;
}

static bool
created_directory_same(const struct stat *a, const struct stat *b) {
	return a->st_dev == b->st_dev && a->st_ino == b->st_ino && a->st_uid == b->st_uid &&
		   a->st_mode == b->st_mode;
}

static bool
created_databases(int base) {
	DIR *dir;
	struct dirent *entry;
	unsigned found = 0;
	bool ok = true;
	int fd = openat(base, ".", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0)
		return false;
	dir = fdopendir(fd);
	if (!dir) {
		(void)close(fd);
		return false;
	}
	errno = 0;
	while ((entry = readdir(dir)) != NULL) {
		unsigned bit;
		if (!strcmp(entry->d_name, ".") || !strcmp(entry->d_name, ".."))
			continue;
		bit = !strcmp(entry->d_name, "1")   ? 1
			  : !strcmp(entry->d_name, "4") ? 2
			  : !strcmp(entry->d_name, "5") ? 4
											: 0;
		if (!bit || (found & bit)) {
			ok = false;
			break;
		}
		found |= bit;
	}
	if (errno || found != 7)
		ok = false;
	if (closedir(dir))
		ok = false;
	return ok;
}

static bool
created_directories_current(const PgracInitdbRecoveryContext *ctx, int data_fd,
										int shared_fd, const int dirs[5],
										const struct stat states[5]) {
	static const char *const names[5] = {"global", "base", "1", "4", "5"};
	struct stat data, shared, held, named;
	if (!directory(data_fd, &data) || !directory(shared_fd, &shared) ||
		ctx->data_device != data.st_dev || ctx->data_inode != data.st_ino ||
		ctx->shared_device != shared.st_dev || ctx->shared_inode != shared.st_ino)
		return false;
	for (unsigned i = 0; i < 5; ++i) {
		int parent = i < 2 ? shared_fd : dirs[1];
		if (!directory(dirs[i], &held) ||
			fstatat(parent, names[i], &named, AT_SYMLINK_NOFOLLOW) ||
			!created_directory_same(&held, &states[i]) ||
			!created_directory_same(&held, &named))
			return false;
	}
	return created_databases(dirs[1]);
}

static bool
created_relmap_observations(const PgracInitdbRecoveryContext *ctx, int data_fd,
										int shared_fd, bool synchronize,
										ClusterRecoveryCheckpointFileV2 out[8]) {
	static const char *const names[5] = {"global", "base", "1", "4", "5"};
	PgracInitdbRecoveryContext original;
	ClusterRecoveryFileIdentityV2 ids[4];
	ClusterRecoveryCheckpointFileV2 observations[8] = {0};
	ClusterRelmapFilePairV2 pair;
	struct stat states[5], data, shared;
	int dirs[5] = {-1, -1, -1, -1, -1};
	bool ok = false;
	if (!ctx || !out || overlaps(ctx, sizeof(*ctx), out, sizeof(observations)))
		return false;
	memcpy(&original, ctx, sizeof(original));
	if (original.creator_pid != (uint64)getpid() ||
		!pgrac_initdb_recovery_relmaps_v2(&original, ids) || !directory(data_fd, &data) ||
		!directory(shared_fd, &shared) || original.data_device != data.st_dev ||
		original.data_inode != data.st_ino || original.shared_device != shared.st_dev ||
		original.shared_inode != shared.st_ino)
		return false;
	for (unsigned i = 0; i < 5; ++i) {
		int parent = i < 2 ? shared_fd : dirs[1];
		dirs[i] = openat(parent, names[i], O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (dirs[i] < 0 || !directory(dirs[i], &states[i]))
			goto done;
	}
	if (!created_directories_current(&original, data_fd, shared_fd, dirs, states))
		goto done;
	for (unsigned i = 0; i < 4; ++i) {
		int dir = dirs[i ? i + 1 : 0];
		if (!(synchronize ? cluster_relmap_file_pair_checkpoint_v2(dir, &ids[i], &pair)
						  : cluster_relmap_file_pair_read_v2(dir, &ids[i], &pair)) ||
			pair.view.committed_generation != 1 || pair.view.has_pending ||
			pair.view.used_backup ||
			memcmp(pair.bytes[0], pair.bytes[1], sizeof(pair.bytes[0])) ||
			!created_directories_current(&original, data_fd, shared_fd, dirs, states))
			goto done;
		for (unsigned copy = 0; copy < 2; ++copy) {
			ClusterRecoveryCheckpointFileV2 *f = &observations[2 * i + copy];
			pg_sha256_ctx hash;
			f->identity = ids[i];
			f->copy_index = copy;
			f->copy_count = 2;
			f->synced_bytes = sizeof(pair.bytes[copy]);
			f->page_count = 1;
			pg_sha256_init(&hash);
			pg_sha256_update(&hash, pair.bytes[copy], sizeof(pair.bytes[copy]));
			pg_sha256_final(&hash, f->image_digest);
		}
	}
	ok = memcmp(ctx, &original, sizeof(original)) == 0 &&
		 created_directories_current(&original, data_fd, shared_fd, dirs, states);
done:
	for (int i = 4; i >= 0; --i)
		if (dirs[i] >= 0 && close(dirs[i]))
			ok = false;
	if (ok)
		memcpy(out, observations, sizeof(observations));
	return ok;
}

bool
pgrac_initdb_recovery_relmap_observe_v2(const PgracInitdbRecoveryContext *ctx, int data_fd,
											 int shared_fd,
											 ClusterRecoveryCheckpointFileV2 out[8]) {
	return created_relmap_observations(ctx, data_fd, shared_fd, true, out);
}

bool
pgrac_initdb_recovery_relmap_current_v2(
	const PgracInitdbRecoveryContext *ctx, int data_fd, int shared_fd,
	const ClusterRecoveryCheckpointFileV2 captured[8]) {
	ClusterRecoveryCheckpointFileV2 actual[8];
	uint8 identity[128];
	if (!captured || !created_relmap_observations(ctx, data_fd, shared_fd, false, actual))
		return false;
	for (unsigned i = 0; i < 8; ++i) {
		const ClusterRecoveryCheckpointFileV2 *f = &captured[i], *a = &actual[i];
		if (!cluster_recovery_file_identity_encode_v2(&f->identity, identity,
													  sizeof(identity)) ||
			!cluster_recovery_file_identity_matches_v2(identity, sizeof(identity),
													   &a->identity) ||
			f->copy_index != a->copy_index || f->copy_count != a->copy_count ||
			f->synced_bytes != a->synced_bytes || f->first_page != a->first_page ||
			f->page_count != a->page_count || memcmp(f->image_digest, a->image_digest, 32) ||
			f->source_count != 0 || f->sources != NULL)
			return false;
	}
	return true;
}

#endif
