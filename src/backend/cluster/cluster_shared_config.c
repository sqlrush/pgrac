/*-------------------------------------------------------------------------
 * PGRAC: root-selected immutable configuration objects.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "cluster/cluster_cf_enqueue.h"
#include "cluster/cluster_shared_config.h"
#include "common/cryptohash.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/fd.h"

#define CONFIG_HEADER_CAPACITY 512
#define CONFIG_KEY_CAPACITY (CLUSTER_SHARED_CONFIG_MAX_NAME + 9)

typedef struct ConfigLine {
	char key[CONFIG_KEY_CAPACITY];
	const char *value;
	const char *end;
	size_t decoded_len;
} ConfigLine;

static bool
config_nonzero(const uint8 *bytes, size_t len)
{
	for (size_t i = 0; i < len; ++i)
		if (bytes[i] != 0)
			return true;
	return false;
}

static bool
config_identity_valid(const ClusterSharedConfigIdentity *id)
{
	return id != NULL && id->system_identifier != 0 && id->database_incarnation != 0
		   && id->generation != 0 && config_nonzero(id->storage_uuid, 16)
		   && config_nonzero(id->authority_uuid, 16) && (id->configured[0] || id->configured[1]);
}

static bool
config_ref_valid(const ClusterSharedConfigRef *ref)
{
	return ref != NULL && config_identity_valid(&ref->identity) && config_nonzero(ref->sha256, 32);
}

static void
config_hex(const uint8 *bytes, size_t len, char *out)
{
	static const char hex[] = "0123456789abcdef";
	for (size_t i = 0; i < len; ++i) {
		out[2 * i] = hex[bytes[i] >> 4];
		out[2 * i + 1] = hex[bytes[i] & 15];
	}
	out[2 * len] = '\0';
}

static void
config_bitmap_hex(uint64 value, char out[17])
{
	uint8 bytes[8];
	for (size_t i = 0; i < 8; ++i)
		bytes[i] = value >> (56 - i * 8);
	config_hex(bytes, 8, out);
}

/* Fixed metadata keys precede all user keys in bytewise lexical order. The
 * canonical header also rejects alternate numeric/UUID spellings and unknown
 * metadata, rather than accepting two names for the same immutable object.
 */
static size_t
config_header(const ClusterSharedConfigIdentity *id, char bytes[CONFIG_HEADER_CAPACITY])
{
	char authority[33], storage[33], first[17], second[17];
	int len;

	if (!config_identity_valid(id))
		return 0;
	config_hex(id->authority_uuid, 16, authority);
	config_hex(id->storage_uuid, 16, storage);
	config_bitmap_hex(id->configured[0], first);
	config_bitmap_hex(id->configured[1], second);
	len = snprintf(bytes, CONFIG_HEADER_CAPACITY,
				   "@authority_uuid=%s\n@configured_0=%s\n@configured_1=%s\n"
				   "@database_incarnation=" UINT64_FORMAT "\n@format=1\n"
				   "@generation=" UINT64_FORMAT "\n@storage_uuid=%s\n"
				   "@system_identifier=" UINT64_FORMAT "\n",
				   authority, first, second, id->database_incarnation, id->generation, storage,
				   id->system_identifier);
	return len > 0 && len < CONFIG_HEADER_CAPACITY ? (size_t)len : 0;
}

static bool
config_name_valid(const char *name, size_t len)
{
	bool start = true;
	if (len == 0 || len > CLUSTER_SHARED_CONFIG_MAX_NAME)
		return false;
	for (size_t i = 0; i < len; ++i) {
		unsigned char ch = name[i];
		if (ch >= 'a' && ch <= 'z')
			start = false;
		else if (!start && ((ch >= '0' && ch <= '9') || ch == '_'))
			continue;
		else if (!start && ch == '.')
			start = true;
		else
			return false;
	}
	return !start;
}

static bool
config_node_valid(const ClusterSharedConfigIdentity *id, int node)
{
	return node == CLUSTER_SHARED_CONFIG_COMMON
		   || (node >= 0 && node < CLUSTER_CONTROL_ROOT_RECORD_COUNT
			   && (id->configured[node / 64] & (UINT64_C(1) << (node % 64))) != 0);
}

static bool
config_key(const ClusterSharedConfigIdentity *id, int node, const char *name,
		   char key[CONFIG_KEY_CAPACITY])
{
	size_t len;
	if (name == NULL || !config_node_valid(id, node))
		return false;
	len = strnlen(name, CLUSTER_SHARED_CONFIG_MAX_NAME + 1);
	if (!config_name_valid(name, len))
		return false;
	if (node == CLUSTER_SHARED_CONFIG_COMMON)
		snprintf(key, CONFIG_KEY_CAPACITY, "common.%s", name);
	else
		snprintf(key, CONFIG_KEY_CAPACITY, "node%03d.%s", node, name);
	return true;
}

static bool
config_text_valid(const char *bytes, size_t len, bool newlines)
{
	if (pg_encoding_verifymbstr(PG_UTF8, bytes, len) != (int)len)
		return false;
	for (size_t i = 0; i < len; ++i) {
		unsigned char ch = bytes[i];
		if ((ch < 32 && !(newlines && ch == '\n')) || ch == 127)
			return false;
	}
	return true;
}

static bool
config_hash_context(pg_cryptohash_ctx *ctx, const char *bytes, size_t len, uint8 hash[32])
{
	return pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, (const uint8 *)bytes, len) >= 0
		   && pg_cryptohash_final(ctx, hash, 32) >= 0;
}

static bool
config_hash(const char *bytes, size_t len, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;
	if (ctx == NULL)
		return false;
	ok = config_hash_context(ctx, bytes, len, hash);
	pg_cryptohash_free(ctx);
	return ok;
}

/* No allocations and no SQL/config lexer. The only literal escape is a
 * doubled quote. Backslashes are bytes, so neither include nor interpolation
 * nor a second assignment can be hidden inside a value.
 */
static bool
config_next(const char *bytes, size_t len, size_t *offset, const ClusterSharedConfigIdentity *id,
			ConfigLine *line)
{
	const char *begin = bytes + *offset;
	const char *newline = memchr(begin, '\n', len - *offset);
	const char *equals, *name;
	size_t key_len, name_len;
	int node;

	if (newline == NULL)
		return false;
	equals = memchr(begin, '=', newline - begin);
	if (equals == NULL)
		return false;
	key_len = equals - begin;
	if (key_len >= sizeof(line->key))
		return false;
	if (key_len > 7 && memcmp(begin, "common.", 7) == 0) {
		node = CLUSTER_SHARED_CONFIG_COMMON;
		name = begin + 7;
	} else if (key_len > 8 && memcmp(begin, "node", 4) == 0 && begin[7] == '.') {
		node = 0;
		for (int i = 4; i < 7; ++i) {
			if (begin[i] < '0' || begin[i] > '9')
				return false;
			node = node * 10 + begin[i] - '0';
		}
		name = begin + 8;
	} else
		return false;
	name_len = equals - name;
	if (!config_node_valid(id, node) || !config_name_valid(name, name_len) || newline - equals < 3
		|| equals[1] != '\'' || newline[-1] != '\'')
		return false;
	memcpy(line->key, begin, key_len);
	line->key[key_len] = '\0';
	line->value = equals + 2;
	line->end = newline - 1;
	line->decoded_len = 0;
	for (const char *p = line->value; p < line->end; ++p) {
		if (*p == '\'' && (++p == line->end || *p != '\''))
			return false;
		if (++line->decoded_len > CLUSTER_SHARED_CONFIG_MAX_VALUE)
			return false;
	}
	*offset = newline - bytes + 1;
	return true;
}

ClusterControlRootResult
cluster_shared_config_encode(const ClusterSharedConfigIdentity *id,
							 const ClusterSharedConfigEntry *entries, size_t count, char *bytes,
							 size_t capacity, size_t *len, uint8 sha256[32])
{
	char header[CONFIG_HEADER_CAPACITY], previous[CONFIG_KEY_CAPACITY] = { 0 };
	size_t used, header_len;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;

	if (len != NULL)
		*len = 0;
	if (sha256 != NULL)
		memset(sha256, 0, 32);
	if (bytes != NULL && capacity != 0)
		bytes[0] = '\0';
	if (bytes == NULL || len == NULL || sha256 == NULL || capacity == 0
		|| capacity > CLUSTER_SHARED_CONFIG_MAX_BYTES + 1 || !config_identity_valid(id)
		|| count > CLUSTER_SHARED_CONFIG_MAX_ENTRIES || (count != 0 && entries == NULL))
		return result;
	memset(bytes, 0, capacity);
	header_len = config_header(id, header);
	if (header_len == 0 || header_len >= capacity)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	memcpy(bytes, header, header_len);
	used = header_len;
	for (size_t i = 0; i < count; ++i) {
		char key[CONFIG_KEY_CAPACITY];
		size_t key_len, value_len, escaped_len;
		const char *value = entries[i].value;

		if (!config_key(id, entries[i].node_id, entries[i].name, key) || strcmp(previous, key) >= 0
			|| value == NULL)
			goto fail;
		key_len = strlen(key);
		value_len = strnlen(value, CLUSTER_SHARED_CONFIG_MAX_VALUE + 1);
		if (value_len > CLUSTER_SHARED_CONFIG_MAX_VALUE
			|| !config_text_valid(value, value_len, false))
			goto fail;
		escaped_len = value_len;
		for (size_t j = 0; j < value_len; ++j)
			if (value[j] == '\'')
				++escaped_len;
		if (key_len + escaped_len + 4 >= capacity - used) {
			result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
			goto fail;
		}
		memcpy(bytes + used, key, key_len);
		used += key_len;
		bytes[used++] = '=';
		bytes[used++] = '\'';
		for (size_t j = 0; j < value_len; ++j) {
			bytes[used++] = value[j];
			if (value[j] == '\'')
				bytes[used++] = '\'';
		}
		bytes[used++] = '\'';
		bytes[used++] = '\n';
		strlcpy(previous, key, sizeof(previous));
	}
	if (!config_hash(bytes, used, sha256)) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto fail;
	}
	*len = used;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
fail:
	memset(bytes, 0, capacity);
	memset(sha256, 0, 32);
	return result;
}

/* Valid arguments/bounds and verified hash required. No backend allocation;
 * install may call this while holding raw directory descriptors.
 */
static ClusterControlRootResult
config_validate_content(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
						uint32 *count)
{
	char header[CONFIG_HEADER_CAPACITY], previous[CONFIG_KEY_CAPACITY] = { 0 };
	size_t offset;
	uint32 entries = 0;
	ConfigLine line;

	if (!config_text_valid(bytes, len, true))
		return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
	offset = config_header(&ref->identity, header);
	if (offset == 0 || len < offset || memcmp(bytes, header, offset) != 0)
		return CLUSTER_CONTROL_ROOT_IDENTITY_MISMATCH;
	while (offset < len) {
		if (++entries > CLUSTER_SHARED_CONFIG_MAX_ENTRIES
			|| !config_next(bytes, len, &offset, &ref->identity, &line)
			|| strcmp(previous, line.key) >= 0)
			return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
		strlcpy(previous, line.key, sizeof(previous));
	}
	*count = entries;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_shared_config_validate(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
							   uint32 *count)
{
	uint8 hash[32];
	if (count != NULL)
		*count = 0;
	if (bytes == NULL || count == NULL || !config_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (len == 0 || len > CLUSTER_SHARED_CONFIG_MAX_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	if (!config_hash(bytes, len, hash))
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (memcmp(hash, ref->sha256, 32) != 0)
		return CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
	return config_validate_content(bytes, len, ref, count);
}

ClusterControlRootResult
cluster_shared_config_visit(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
							ClusterSharedConfigVisitor visitor, void *arg)
{
	ClusterControlRootResult result;
	uint32 count;
	size_t offset;
	char header[CONFIG_HEADER_CAPACITY];
	char value[CLUSTER_SHARED_CONFIG_MAX_VALUE + 1];
	ConfigLine line;

	if (visitor == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_shared_config_validate(bytes, len, ref, &count);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	offset = config_header(&ref->identity, header);
	while (offset < len) {
		ClusterSharedConfigEntry entry;
		size_t used = 0;
		if (!config_next(bytes, len, &offset, &ref->identity, &line))
			return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
		if (line.key[0] == 'c') {
			entry.node_id = CLUSTER_SHARED_CONFIG_COMMON;
			entry.name = line.key + 7;
		} else {
			entry.node_id
				= (line.key[4] - '0') * 100 + (line.key[5] - '0') * 10 + line.key[6] - '0';
			entry.name = line.key + 8;
		}
		for (const char *pos = line.value; pos < line.end; ++pos) {
			value[used++] = *pos;
			if (*pos == '\'')
				++pos;
		}
		value[used] = '\0';
		entry.value = value;
		result = visitor(&entry, arg);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			return result;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

/* PGRAC: preserve canonical unrelated entries byte-for-byte. This producer
 * has no authority to select the old object or publish the amended one.
 * Author: SqlRush <sqlrush@gmail.com> */
ClusterControlRootResult
cluster_shared_config_amend(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
							const ClusterSharedConfigEntry *change, ClusterSharedConfigImage *out,
							ClusterSharedConfigRef *next_ref, bool *changed)
{
	ClusterSharedConfigRef next;
	ClusterControlRootResult result;
	ConfigLine line;
	char key[CONFIG_KEY_CAPACITY], header[CONFIG_HEADER_CAPACITY];
	char previous[CLUSTER_SHARED_CONFIG_MAX_VALUE + 1];
	char *updated;
	uint32 count;
	size_t old_header, new_header, begin, end, offset, used, result_len;
	size_t value_len = 0, escaped_len = 0, replacement_len = 0;
	bool found = false, modifies;

	/* Do not erase a caller's input while rejecting an obvious carrier alias. */
	if ((const void *)ref == (void *)next_ref && ref != NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (next_ref != NULL)
		memset(next_ref, 0, sizeof(*next_ref));
	if (changed != NULL)
		*changed = false;
	if (out == NULL || next_ref == NULL || changed == NULL || change == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_shared_config_validate(bytes, len, ref, &count);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	if (!config_key(&ref->identity, change->node_id, change->name, key))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (change->value != NULL) {
		value_len = strnlen(change->value, CLUSTER_SHARED_CONFIG_MAX_VALUE + 1);
		if (value_len > CLUSTER_SHARED_CONFIG_MAX_VALUE
			|| !config_text_valid(change->value, value_len, false))
			return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
		escaped_len = value_len;
		for (size_t i = 0; i < value_len; i++)
			if (change->value[i] == '\'')
				escaped_len++;
		replacement_len = strlen(key) + escaped_len + 4;
	}
	old_header = config_header(&ref->identity, header);
	begin = end = len;
	offset = old_header;
	while (offset < len) {
		size_t start = offset;
		int order;
		if (!config_next(bytes, len, &offset, &ref->identity, &line))
			return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
		order = strcmp(line.key, key);
		if (order >= 0) {
			begin = end = start;
			if (order == 0) {
				found = true;
				end = offset;
				used = 0;
				for (const char *p = line.value; p < line.end; p++) {
					previous[used++] = *p;
					if (*p == '\'')
						p++;
				}
				previous[used] = '\0';
			}
			break;
		}
	}
	modifies = change->value != NULL ? !found || strcmp(previous, change->value) != 0 : found;
	if (!modifies) {
		updated = palloc(len + 1);
		memcpy(updated, bytes, len);
		updated[len] = '\0';
		out->bytes = updated;
		out->len = len;
		*next_ref = *ref;
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	if (ref->identity.generation == UINT64_MAX)
		return CLUSTER_CONTROL_ROOT_SEQUENCE_EXHAUSTED;
	if (!found && change->value != NULL && count == CLUSTER_SHARED_CONFIG_MAX_ENTRIES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	next = *ref;
	next.identity.generation++;
	new_header = config_header(&next.identity, header);
	result_len = new_header + (len - old_header) - (end - begin) + replacement_len;
	if (result_len > CLUSTER_SHARED_CONFIG_MAX_BYTES)
		return CLUSTER_CONTROL_ROOT_BAD_SIZE;
	updated = palloc(result_len + 1);
	memcpy(updated, header, new_header);
	used = new_header;
	memcpy(updated + used, bytes + old_header, begin - old_header);
	used += begin - old_header;
	if (change->value != NULL) {
		size_t key_len = strlen(key);
		memcpy(updated + used, key, key_len);
		used += key_len;
		updated[used++] = '=';
		updated[used++] = '\'';
		for (size_t i = 0; i < value_len; i++) {
			updated[used++] = change->value[i];
			if (change->value[i] == '\'')
				updated[used++] = '\'';
		}
		updated[used++] = '\'';
		updated[used++] = '\n';
	}
	memcpy(updated + used, bytes + end, len - end);
	used += len - end;
	updated[used] = '\0';
	if (used != result_len || !config_hash(updated, used, next.sha256)) {
		pfree(updated);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	result = config_validate_content(updated, used, &next, &count);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		pfree(updated);
		return result;
	}
	out->bytes = updated;
	out->len = used;
	*next_ref = next;
	*changed = true;
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

ClusterControlRootResult
cluster_shared_config_lookup(const char *bytes, size_t len, const ClusterSharedConfigRef *ref,
							 int node_id, const char *name, char *value, size_t capacity)
{
	char key[CONFIG_KEY_CAPACITY], header[CONFIG_HEADER_CAPACITY];
	ClusterControlRootResult result;
	uint32 count;
	size_t offset;
	ConfigLine line;

	if (value != NULL && capacity != 0)
		value[0] = '\0';
	if (value == NULL || capacity == 0 || !config_ref_valid(ref)
		|| !config_key(&ref->identity, node_id, name, key))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_shared_config_validate(bytes, len, ref, &count);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	offset = config_header(&ref->identity, header);
	while (offset < len) {
		size_t used = 0;
		if (!config_next(bytes, len, &offset, &ref->identity, &line))
			return CLUSTER_CONTROL_ROOT_BAD_RESERVED;
		if (strcmp(line.key, key) != 0)
			continue;
		if (line.decoded_len >= capacity)
			return CLUSTER_CONTROL_ROOT_BAD_SIZE;
		for (const char *p = line.value; p < line.end; ++p) {
			value[used++] = *p;
			if (*p == '\'')
				++p;
		}
		value[used] = '\0';
		return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
	}
	return CLUSTER_CONTROL_ROOT_ABSENT;
}

static bool
config_owned(const struct stat *st, bool directory)
{
	return (directory ? S_ISDIR(st->st_mode) : S_ISREG(st->st_mode)) && st->st_uid == geteuid()
		   && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

ClusterControlRootResult
cluster_shared_config_read_locked(const char *shared_root, const ClusterSharedConfigRef *ref,
								  ClusterSharedConfigImage *out)
{
	const int dirflags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	const char *parts[] = { "global", "config_images" };
	int fds[4] = { -1, -1, -1, -1 };
	char hex[65], name[96];
	char *bytes;
	struct stat st;
	size_t used = 0, expected = 0;
	uint32 count;
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (shared_root == NULL || shared_root[0] != '/' || !config_ref_valid(ref))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ShareLock)
		&& !cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	config_hex(ref->sha256, 32, hex);
	snprintf(name, sizeof(name), UINT64_FORMAT "-%s.conf", ref->identity.generation, hex);
	/* Fallible backend allocation precedes every raw FD; hashing/parsing is
	 * after closing them. No backend ERROR can leak an open raw descriptor.
	 */
	bytes = palloc(CLUSTER_SHARED_CONFIG_MAX_BYTES + 1);
	fds[0] = open(shared_root, dirflags);
	if (fds[0] < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fds[0], &st) != 0 || !config_owned(&st, true))
		goto done;
	for (size_t i = 0; i < lengthof(parts); ++i) {
		fds[i + 1] = openat(fds[i], parts[i], dirflags);
		if (fds[i + 1] < 0) {
			if (errno == ENOENT)
				result = CLUSTER_CONTROL_ROOT_ABSENT;
			goto done;
		}
		if (fstat(fds[i + 1], &st) != 0 || !config_owned(&st, true))
			goto done;
	}
	fds[3] = openat(fds[2], name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fds[3] < 0) {
		if (errno == ENOENT)
			result = CLUSTER_CONTROL_ROOT_ABSENT;
		goto done;
	}
	if (fstat(fds[3], &st) != 0 || !config_owned(&st, false))
		goto done;
	if (st.st_size <= 0 || st.st_size > CLUSTER_SHARED_CONFIG_MAX_BYTES) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	expected = st.st_size;
	/* Observe EOF, not only a stat size. Concurrent extra bytes are refused. */
	while (used < expected + 1) {
		ssize_t n = read(fds[3], bytes + used, expected + 1 - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	result = used == expected ? CLUSTER_CONTROL_ROOT_OK_PRIMARY : CLUSTER_CONTROL_ROOT_BAD_SIZE;
done:
	for (size_t i = 0; i < lengthof(fds); ++i)
		if (fds[i] >= 0 && close(fds[i]) != 0)
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		result = cluster_shared_config_validate(bytes, used, ref, &count);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY) {
		bytes[used] = '\0';
		out->bytes = bytes;
		out->len = used;
	} else
		pfree(bytes);
	return result;
}

void
cluster_shared_config_free(ClusterSharedConfigImage *image)
{
	if (image != NULL) {
		if (image->bytes != NULL)
			pfree(image->bytes);
		memset(image, 0, sizeof(*image));
	}
}

#define CONFIG_STAGED 1
#define CONFIG_INSTALLED 2
#define CONFIG_DISCARDED 3

typedef struct ConfigStageDirs {
	int fd[4]; /* root, global, objects, staging */
	struct stat objects;
	struct stat staging;
} ConfigStageDirs;

static void
config_close(int fd, ClusterControlRootResult *result)
{
	if (fd >= 0 && close(fd) != 0)
		*result = CLUSTER_CONTROL_ROOT_IO_ERROR;
}

static void
config_close_stage_dirs(ConfigStageDirs *dirs, ClusterControlRootResult *result)
{
	for (int i = 3; i >= 0; --i) {
		config_close(dirs->fd[i], result);
		dirs->fd[i] = -1;
	}
}

/* Caller closes on EVERY return. No allocations/error-throwing backend calls. */
static ClusterControlRootResult
config_open_stage_dirs(const char *root, ConfigStageDirs *dirs)
{
	const char *parts[] = { "global", "config_images", ".staging" };
	const int flags = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
	struct stat st;
	memset(dirs, 0, sizeof(*dirs));
	for (int i = 0; i < 4; ++i)
		dirs->fd[i] = -1;
	if (root == NULL || root[0] != '/')
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	for (int i = 0; i < 4; ++i) {
		dirs->fd[i] = i == 0 ? open(root, flags) : openat(dirs->fd[i - 1], parts[i - 1], flags);
		if (dirs->fd[i] < 0 || fstat(dirs->fd[i], &st) != 0 || !config_owned(&st, true))
			return CLUSTER_CONTROL_ROOT_IO_ERROR;
		if (i == 2)
			dirs->objects = st;
		if (i == 3)
			dirs->staging = st;
	}
	return CLUSTER_CONTROL_ROOT_OK_PRIMARY;
}

static void
config_stage_names(const ClusterSharedConfigStage *stage, char final[96], char temp[40])
{
	char hex[65];
	config_hex(stage->ref.sha256, 32, hex);
	snprintf(final, 96, UINT64_FORMAT "-%s.conf", stage->ref.identity.generation, hex);
	config_hex(stage->operation_uuid, 16, hex);
	snprintf(temp, 40, "%s.tmp", hex);
}

static bool
config_stage_valid(const ClusterSharedConfigStage *stage)
{
	return stage != NULL && config_ref_valid(&stage->ref)
		   && config_nonzero(stage->operation_uuid, 16) && stage->owner_pid == (uint32)getpid()
		   && stage->bytes > 0 && stage->bytes <= CLUSTER_SHARED_CONFIG_MAX_BYTES
		   && (stage->state == CONFIG_STAGED || stage->state == CONFIG_INSTALLED
			   || stage->state == CONFIG_DISCARDED);
}

static bool
config_dirs_match(const ConfigStageDirs *dirs, const ClusterSharedConfigStage *stage)
{
	return (uint64)dirs->objects.st_dev == stage->object_dir_dev
		   && (uint64)dirs->objects.st_ino == stage->object_dir_ino
		   && (uint64)dirs->staging.st_dev == stage->staging_dir_dev
		   && (uint64)dirs->staging.st_ino == stage->staging_dir_ino;
}

static bool
config_exact_temp(int dir, const char *temp, const ClusterSharedConfigStage *stage)
{
	struct stat st;
	return fstatat(dir, temp, &st, AT_SYMLINK_NOFOLLOW) == 0 && config_owned(&st, false)
		   && (uint64)st.st_dev == stage->file_dev && (uint64)st.st_ino == stage->file_ino;
}

/* Buffer capacity is stage->bytes + 1. The crypto context was allocated
 * before opening any raw FD. All further validation is allocation-free.
 */
static ClusterControlRootResult
config_stage_read_at(int dir, const char *name, const ClusterSharedConfigStage *stage,
					 bool exact_inode, char *bytes, pg_cryptohash_ctx *ctx)
{
	ClusterControlRootResult result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	struct stat st;
	uint8 hash[32];
	size_t used = 0;
	uint32 count = 0;
	int fd = openat(dir, name, O_RDONLY | O_NOFOLLOW | O_CLOEXEC | O_NONBLOCK | PG_BINARY);
	if (fd < 0)
		return errno == ENOENT ? CLUSTER_CONTROL_ROOT_ABSENT : result;
	if (fstat(fd, &st) != 0 || !config_owned(&st, false))
		goto done;
	if (exact_inode
		&& ((uint64)st.st_dev != stage->file_dev || (uint64)st.st_ino != stage->file_ino)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (st.st_size < 0 || (uint64)st.st_size != stage->bytes) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	while (used < stage->bytes + 1) {
		ssize_t n = read(fd, bytes + used, stage->bytes + 1 - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0)
			goto done;
		if (n == 0)
			break;
		used += n;
	}
	if (used != stage->bytes) {
		result = CLUSTER_CONTROL_ROOT_BAD_SIZE;
		goto done;
	}
	if (!config_hash_context(ctx, bytes, used, hash))
		goto done;
	if (memcmp(hash, stage->ref.sha256, 32) != 0) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	result = config_validate_content(bytes, used, &stage->ref, &count);
done:
	config_close(fd, &result);
	return result;
}

ClusterControlRootResult
cluster_shared_config_prepare(const char *shared_root, const char *bytes, size_t len,
							  const ClusterSharedConfigRef *ref, const uint8 operation_uuid[16],
							  ClusterSharedConfigStage *out)
{
	ClusterSharedConfigStage stage;
	ClusterControlRootResult result;
	ConfigStageDirs dirs;
	char final[96], temp[40];
	struct stat st;
	uint32 count;
	size_t used = 0;
	int fd = -1;

	if (out == NULL)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!enableFsync || operation_uuid == NULL || !config_nonzero(operation_uuid, 16))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	result = cluster_shared_config_validate(bytes, len, ref, &count);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		return result;
	memset(&stage, 0, sizeof(stage));
	stage.ref = *ref;
	stage.bytes = len;
	stage.owner_pid = (uint32)getpid();
	memcpy(stage.operation_uuid, operation_uuid, 16);
	config_stage_names(&stage, final, temp);
	result = config_open_stage_dirs(shared_root, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	stage.object_dir_dev = dirs.objects.st_dev;
	stage.object_dir_ino = dirs.objects.st_ino;
	stage.staging_dir_dev = dirs.staging.st_dev;
	stage.staging_dir_ino = dirs.staging.st_ino;
	result = CLUSTER_CONTROL_ROOT_IO_ERROR;
	fd = openat(dirs.fd[3], temp, O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC | PG_BINARY,
				0600);
	if (fd < 0 || fstat(fd, &st) != 0 || !config_owned(&st, false))
		goto done;
	stage.file_dev = st.st_dev;
	stage.file_ino = st.st_ino;
	stage.state = CONFIG_STAGED;
	while (used < len) {
		ssize_t n = write(fd, bytes + used, len - used);
		if (n < 0 && errno == EINTR)
			continue;
		if (n <= 0)
			goto done;
		used += n;
	}
	if (pg_fsync(fd) != 0 || pg_fsync(dirs.fd[3]) != 0)
		goto done;
	result = CLUSTER_CONTROL_ROOT_OK_PRIMARY;
done:
	config_close(fd, &result);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY && stage.state == CONFIG_STAGED
		&& config_exact_temp(dirs.fd[3], temp, &stage)) {
		if (unlinkat(dirs.fd[3], temp, 0) == 0)
			(void)pg_fsync(dirs.fd[3]);
	}
	config_close_stage_dirs(&dirs, &result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		*out = stage;
	return result;
}

ClusterControlRootResult
cluster_shared_config_install(const char *shared_root, ClusterSharedConfigStage *stage)
{
	ClusterControlRootResult result;
	ConfigStageDirs dirs;
	char final[96], temp[40];
	char *staged, *installed;
	pg_cryptohash_ctx *ctx;

	if (!enableFsync || !config_stage_valid(stage) || stage->state == CONFIG_DISCARDED)
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	if (!cluster_cf_held_is_clusterwide(ExclusiveLock))
		return CLUSTER_CONTROL_ROOT_LOCK_UNAVAILABLE;
	staged = palloc(stage->bytes + 1);
	installed = palloc(stage->bytes + 1);
	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL) {
		pfree(staged);
		pfree(installed);
		return CLUSTER_CONTROL_ROOT_IO_ERROR;
	}
	config_stage_names(stage, final, temp);
	result = config_open_stage_dirs(shared_root, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (!config_dirs_match(&dirs, stage)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (stage->state == CONFIG_STAGED) {
		result = config_stage_read_at(dirs.fd[3], temp, stage, true, staged, ctx);
		if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
			goto done;
		if (linkat(dirs.fd[3], temp, dirs.fd[2], final, 0) != 0 && errno != EEXIST) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
	}
	result = config_stage_read_at(dirs.fd[2], final, stage, false, installed, ctx);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (stage->state == CONFIG_STAGED && memcmp(staged, installed, stage->bytes) != 0) {
		result = CLUSTER_CONTROL_ROOT_HASH_MISMATCH;
		goto done;
	}
	if (pg_fsync(dirs.fd[2]) != 0) {
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
		goto done;
	}
	if (stage->state == CONFIG_STAGED) {
		if (!config_exact_temp(dirs.fd[3], temp, stage)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (unlinkat(dirs.fd[3], temp, 0) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
		/* Failed directory sync must retry sync, not resurrect a removed temp. */
		stage->state = CONFIG_INSTALLED;
	}
	if (pg_fsync(dirs.fd[3]) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
done:
	config_close_stage_dirs(&dirs, &result);
	pg_cryptohash_free(ctx);
	pfree(staged);
	pfree(installed);
	return result;
}

ClusterControlRootResult
cluster_shared_config_discard(const char *shared_root, ClusterSharedConfigStage *stage)
{
	ClusterControlRootResult result;
	ConfigStageDirs dirs;
	char final[96], temp[40];
	if (!enableFsync || !config_stage_valid(stage))
		return CLUSTER_CONTROL_ROOT_INVALID_ARGUMENT;
	config_stage_names(stage, final, temp);
	result = config_open_stage_dirs(shared_root, &dirs);
	if (result != CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		goto done;
	if (!config_dirs_match(&dirs, stage)) {
		result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
		goto done;
	}
	if (stage->state == CONFIG_STAGED) {
		if (!config_exact_temp(dirs.fd[3], temp, stage)) {
			result = CLUSTER_CONTROL_ROOT_STALE_TOKEN;
			goto done;
		}
		if (unlinkat(dirs.fd[3], temp, 0) != 0) {
			result = CLUSTER_CONTROL_ROOT_IO_ERROR;
			goto done;
		}
	}
	/* Even an INSTALLED descriptor is terminal before the fallible sync. */
	stage->state = CONFIG_DISCARDED;
	if (pg_fsync(dirs.fd[3]) != 0)
		result = CLUSTER_CONTROL_ROOT_IO_ERROR;
done:
	config_close_stage_dirs(&dirs, &result);
	if (result == CLUSTER_CONTROL_ROOT_OK_PRIMARY)
		memset(stage, 0, sizeof(*stage));
	return result;
}
