/* Same-build original-creator delivery. No durable format or ROOT authority. */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif
#include "common/pgrac_initdb_recovery.h"
#ifdef WIN32
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
#else
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <sys/stat.h>
#include <unistd.h>
#include "catalog/pg_database_d.h"
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
pgrac_initdb_recovery_pipe_v2(const PgracInitdbRecoveryContext *ctx, int *out)
{
	ClusterRecoveryFileIdentityV2 ids[4];
	int fds[2];
	ssize_t n;
	bool ok;
	if (!out || !ctx || overlaps(out, sizeof(*out), ctx, sizeof(*ctx))
		|| !pgrac_initdb_recovery_relmaps_v2(ctx, ids) || pipe(fds) != 0)
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
		|| !pgrac_initdb_recovery_relmaps_v2(&ctx, ids) || !directory(data_fd, &data)
		|| !directory(shared_fd, &shared) || ctx.data_device != data.st_dev
		|| ctx.data_inode != data.st_ino || ctx.shared_device != shared.st_dev
		|| ctx.shared_inode != shared.st_ino || ctx.creator_pid != (uint64)getppid())
		goto done;
	ok = true;
done:
	if (close(fd) != 0)
		ok = false;
	if (ok)
		*out = ctx;
	return ok;
}

#endif
