/* Original initdb's immutable initial configuration, before ROOT exists.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_initdb_config.h"
#include "cluster/cluster_shared_config.h"
#include "common/cryptohash.h"
#include "common/file_perm.h"
#include "miscadmin.h"

StaticAssertDecl(PGRAC_INITDB_CONFIG_MAX_BYTES == CLUSTER_SHARED_CONFIG_MAX_BYTES,
				 "creation uses the existing configuration size bound");

struct ClusterInitdbConfig {
	ClusterSharedConfigRef ref;
	Size len;
	char *bytes;
	char paths[3][MAXPGPATH];
};

static void
pg_attribute_noreturn() config_refuse(const char *reason)
{
	ereport(FATAL,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("INITDB_CONFIG_CREATE: %s", reason)));
	pg_unreachable();
}

/* This parser discovers only a REQUEST's identity. The existing full codec
 * then checks canonical spelling, sort order, limits and the entire digest.
 * Root-selected readers continue to require their independently selected ref. */
static void
request_field(const ClusterInitdbConfig *config, Size *offset, const char *key, char *value,
			  Size capacity)
{
	Size keylen = strlen(key), n;
	const char *end;
	if (*offset > config->len || config->len - *offset <= keylen
		|| memcmp(config->bytes + *offset, key, keylen) != 0)
		config_refuse("configuration request metadata is invalid");
	*offset += keylen;
	end = memchr(config->bytes + *offset, '\n', config->len - *offset);
	if (end == NULL || (n = end - (config->bytes + *offset)) == 0 || n >= capacity)
		config_refuse("configuration request metadata is incomplete");
	memcpy(value, config->bytes + *offset, n);
	value[n] = '\0';
	*offset += n + 1;
}

static uint64
request_number(const char *value)
{
	uint64 result = 0;
	for (const char *p = value; *p; p++) {
		if (*p < '0' || *p > '9' || result > (UINT64_MAX - (*p - '0')) / 10)
			config_refuse("configuration request number is invalid");
		result = result * 10 + (*p - '0');
	}
	return result;
}

static void
request_hex(const char *value, uint8 *out, Size count)
{
	static const char hex[] = "0123456789abcdef";
	if (strlen(value) != count * 2)
		config_refuse("configuration request identity is invalid");
	for (Size i = 0; i < count * 2; i++) {
		const char *digit = strchr(hex, value[i]);
		if (digit == NULL)
			config_refuse("configuration request identity is invalid");
		out[i / 2] = (out[i / 2] << 4) | (digit - hex);
	}
}

static bool
owned_file(const struct stat *st, const PgracInitdbConfigContext *source)
{
	return S_ISREG(st->st_mode) && st->st_uid == geteuid() && st->st_nlink == 1
		   && (st->st_mode & 0022) == 0 && st->st_size == source->bytes
		   && (uint64)st->st_dev == source->device && (uint64)st->st_ino == source->inode;
}

ClusterInitdbConfig *
cluster_initdb_config_preflight(const PgracInitdbConfigContext *source)
{
	ClusterInitdbConfig *config;
	ClusterSharedConfigIdentity *id;
	ClusterSharedConfigPolicyReport policy;
	struct stat before, after;
	pg_cryptohash_ctx *hash;
	char value[64], data[MAXPGPATH], wal[MAXPGPATH], undo[MAXPGPATH], extra;
	uint8 bitmap[8], digest[32];
	Size offset = 0, used = 0;
	uint32 count;
	ssize_t n;

	if (IsUnderPostmaster || source == NULL || source->fd < 3 || source->bytes == 0
		|| source->bytes > CLUSTER_SHARED_CONFIG_MAX_BYTES || fstat(source->fd, &before) != 0
		|| !owned_file(&before, source))
		config_refuse("configuration request is not bound to the original creator");
	config = palloc0(sizeof(*config));
	config->bytes = palloc(source->bytes + 1);
	config->len = source->bytes;
	while (used < config->len) {
		n = pread(source->fd, config->bytes + used, config->len - used, used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			config_refuse("cannot read the original configuration request");
		used += n;
	}
	do {
		n = pread(source->fd, &extra, 1, used);
	} while (n < 0 && errno == EINTR);
	if (n != 0 || fstat(source->fd, &after) != 0 || !owned_file(&after, source)
		|| before.st_mtime != after.st_mtime || before.st_ctime != after.st_ctime)
		config_refuse("configuration request changed while being read");
	config->bytes[used] = '\0';
	hash = pg_cryptohash_create(PG_SHA256);
	if (hash == NULL || pg_cryptohash_init(hash) < 0
		|| pg_cryptohash_update(hash, (uint8 *)config->bytes, used) < 0
		|| pg_cryptohash_final(hash, digest, sizeof(digest)) < 0)
		config_refuse("cannot verify the configuration request digest");
	pg_cryptohash_free(hash);
	if (memcmp(digest, source->sha256, 32) != 0)
		config_refuse("configuration request differs from the original input");
	memcpy(config->ref.sha256, digest, 32);
	id = &config->ref.identity;
	request_field(config, &offset, "@authority_uuid=", value, sizeof(value));
	request_hex(value, id->authority_uuid, 16);
	for (int i = 0; i < 2; i++) {
		memset(bitmap, 0, sizeof(bitmap));
		request_field(config, &offset, i == 0 ? "@configured_0=" : "@configured_1=", value,
					  sizeof(value));
		request_hex(value, bitmap, sizeof(bitmap));
		for (int j = 0; j < sizeof(bitmap); j++)
			id->configured[i] = (id->configured[i] << 8) | bitmap[j];
	}
	request_field(config, &offset, "@database_incarnation=", value, sizeof(value));
	id->database_incarnation = request_number(value);
	request_field(config, &offset, "@format=", value, sizeof(value));
	if (strcmp(value, "1") != 0)
		config_refuse("configuration request format is unsupported");
	request_field(config, &offset, "@generation=", value, sizeof(value));
	id->generation = request_number(value);
	request_field(config, &offset, "@storage_uuid=", value, sizeof(value));
	request_hex(value, id->storage_uuid, 16);
	request_field(config, &offset, "@system_identifier=", value, sizeof(value));
	id->system_identifier = request_number(value);
	if (id->generation != 1 || (id->configured[0] & 1) == 0
		|| cluster_shared_config_validate(config->bytes, config->len, &config->ref, &count)
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		config_refuse("configuration request is not a canonical first input for this new database");

	if (cluster_shared_config_lookup(config->bytes, config->len, &config->ref, -1,
									 "cluster.shared_data_dir", data, sizeof(data))
			!= CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_shared_config_lookup(config->bytes, config->len, &config->ref, -1,
										"cluster.wal_threads_dir", wal, sizeof(wal))
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| cluster_shared_config_lookup(config->bytes, config->len, &config->ref, -1,
										"cluster.undo_tablespace_path", undo, sizeof(undo))
			   != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		config_refuse("configuration request is missing a bootstrap path");
	if (cluster_shared_config_check_bootstrap(config->bytes, config->len, &config->ref, 0, data,
											  wal, undo, &policy)
		!= CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		ereport(FATAL, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("INITDB_CONFIG_CREATE: invalid or incomplete bootstrap profile"),
						errdetail("Configuration reason=%d node=%d parameter=%s.", policy.reason,
								  policy.node_id, policy.name)));
	strlcpy(config->paths[0], data, MAXPGPATH);
	strlcpy(config->paths[1], wal, MAXPGPATH);
	strlcpy(config->paths[2], undo, MAXPGPATH);
	return config;
}

const ClusterSharedConfigRef *
cluster_initdb_config_reference(const ClusterInitdbConfig *config)
{
	return &config->ref;
}

const char *
cluster_initdb_config_path(const ClusterInitdbConfig *config, unsigned index)
{
	Assert(index < lengthof(config->paths));
	return config->paths[index];
}

void
cluster_initdb_config_free(ClusterInitdbConfig *config)
{
	pfree(config->bytes);
	pfree(config);
}

ClusterInitdbConfig *
cluster_initdb_config_prepare(const PgracInitdbWalContext *context)
{
	ClusterInitdbConfig *config;
	const ClusterSharedConfigIdentity *id;
	const char *data;
	char *resolved;
	struct stat target;
	int fd;

	if (context->config.fd == 0)
		return NULL;
	if (IsUnderPostmaster || context->phase != PGRAC_INITDB_WAL_POSTBOOTSTRAP
		|| context->thread_id != 1 || context->base_fd < 3)
		config_refuse("configuration request is not bound to the original creator");
	config = cluster_initdb_config_preflight(&context->config);
	id = &config->ref.identity;
	if (id->system_identifier != context->system_identifier
		|| id->database_incarnation != context->database_incarnation
		|| memcmp(id->storage_uuid, context->storage_uuid, 16) != 0)
		config_refuse("configuration request is not a canonical first input for this new database");
	data = config->paths[0];
	/* Only DATA exists at this creation step. The full cohort owner still
	 * must independently qualify WAL/undo deployment paths before ROOT-last. */
	resolved = realpath(data, NULL);
	if (resolved == NULL || strcmp(resolved, data) != 0)
		config_refuse("configuration DATA path is not canonical");
	free(resolved);
	fd = open(data, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (fd < 0 || fstat(fd, &target) != 0 || close(fd) != 0
		|| (uint64)target.st_dev != context->base_device
		|| (uint64)target.st_ino != context->base_inode)
		config_refuse("configuration DATA path does not name the original new target");
	return config;
}

void
cluster_initdb_config_create(ClusterInitdbConfig *config, int global_fd)
{
	static const char hex[] = "0123456789abcdef";
	char name[72], readback[8192], extra;
	struct stat held, named, dir_st;
	Size used = 0;
	int dir, fd;
	ssize_t n;
	if (config == NULL)
		return;
	if (mkdirat(global_fd, "config_images", pg_dir_create_mode) != 0)
		config_refuse("cannot exclusively create the initial configuration directory");
	dir = openat(global_fd, "config_images", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (dir < 0 || fstat(dir, &dir_st) != 0 || !S_ISDIR(dir_st.st_mode))
		config_refuse("cannot bind the new configuration directory");
	name[0] = '1';
	name[1] = '-';
	for (int i = 0; i < 32; i++) {
		name[2 + 2 * i] = hex[config->ref.sha256[i] >> 4];
		name[3 + 2 * i] = hex[config->ref.sha256[i] & 15];
	}
	memcpy(name + 66, ".conf", 6);
	fd = openat(dir, name, O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, pg_file_create_mode);
	if (fd < 0)
		config_refuse("cannot exclusively create the initial configuration object");
	while (used < config->len) {
		n = pwrite(fd, config->bytes + used, config->len - used, used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			config_refuse("cannot write the initial configuration object");
		used += n;
	}
	if (fsync(fd) != 0 || fstat(fd, &held) != 0 || !S_ISREG(held.st_mode)
		|| held.st_uid != geteuid() || held.st_nlink != 1 || (held.st_mode & 0022) != 0
		|| held.st_size != config->len)
		config_refuse("cannot persist the initial configuration object");
	used = 0;
	while (used < config->len) {
		Size amount = Min(sizeof(readback), config->len - used);
		n = pread(fd, readback, amount, used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0 || memcmp(readback, config->bytes + used, n) != 0)
			config_refuse("initial configuration readback differs");
		used += n;
	}
	do {
		n = pread(fd, &extra, 1, used);
	} while (n < 0 && errno == EINTR);
	if (n != 0 || fstatat(dir, name, &named, AT_SYMLINK_NOFOLLOW) != 0
		|| named.st_dev != held.st_dev || named.st_ino != held.st_ino
		|| named.st_mode != held.st_mode || named.st_nlink != 1 || named.st_size != held.st_size
		|| close(fd) != 0 || fsync(dir) != 0
		|| fstatat(global_fd, "config_images", &named, AT_SYMLINK_NOFOLLOW) != 0
		|| named.st_dev != dir_st.st_dev || named.st_ino != dir_st.st_ino || !S_ISDIR(named.st_mode)
		|| close(dir) != 0 || fsync(global_fd) != 0)
		config_refuse("cannot complete initial configuration persistence");
	pfree(config->bytes);
	pfree(config);
}
