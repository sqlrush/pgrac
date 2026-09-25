/*-------------------------------------------------------------------------
 * PGRAC: cold-import binding codec and bounded read-only bootstrap I/O.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <fcntl.h>
#include <sys/stat.h>
#ifndef WIN32
#include <unistd.h>
#endif

#include "common/pgrac_control_binding.h"
#include "port/pg_crc32c.h"

StaticAssertDecl(sizeof(PgracControlBinding) == 216, "local binding carrier size");

static bool
binding_zero(const void *ptr, size_t length)
{
	const uint8 *bytes = ptr;

	for (size_t i = 0; i < length; i++)
		if (bytes[i] != 0)
			return false;
	return true;
}

static bool
binding_overlap(const void *left, size_t left_size, const void *right, size_t right_size)
{
	uintptr_t a = (uintptr_t)left;
	uintptr_t b = (uintptr_t)right;

	if (left == NULL || right == NULL || left_size == 0 || right_size == 0)
		return false;
	/* Subtraction avoids overflowing a pointer range's upper endpoint. */
	return a <= b ? b - a < left_size : a - b < right_size;
}

static bool
binding_valid(const PgracControlBinding *binding)
{
	return binding != NULL && binding->system_identifier != 0 && binding->database_incarnation != 0
		   && binding->node_id < PGRAC_CONTROL_BINDING_MAX_NODES && binding->reserved == 0
		   && binding->migration_prepare_generation != 0 && binding->migration_transition_epoch != 0
		   && !binding_zero(binding->storage_uuid, 16) && !binding_zero(binding->authority_uuid, 16)
		   && !binding_zero(binding->operation_uuid, 16)
		   && !binding_zero(binding->source_cold_sha256, 32)
		   && !binding_zero(binding->target_qualification_sha256, 32)
		   && !binding_zero(binding->migration_round_sha256, 32)
		   && !binding_zero(binding->source_wal_state_sha256, 32);
}

static void
binding_put(uint8 *bytes, size_t offset, uint64 value, size_t width)
{
	for (size_t i = 0; i < width; i++)
		bytes[offset + i] = (uint8)(value >> (8 * i));
}

static uint64
binding_get(const uint8 *bytes, size_t offset, size_t width)
{
	uint64 value = 0;

	for (size_t i = 0; i < width; i++)
		value |= (uint64)bytes[offset + i] << (8 * i);
	return value;
}

static pg_crc32c
binding_crc(const uint8 *bytes)
{
	pg_crc32c crc;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, bytes, 248);
	FIN_CRC32C(crc);
	return crc;
}

bool
pgrac_control_binding_encode(const PgracControlBinding *binding, uint8 *bytes, size_t capacity)
{
	bool overlap = binding_overlap(binding, sizeof(*binding), bytes,
								   Min(capacity, PGRAC_CONTROL_BINDING_BYTES));

	if (bytes != NULL)
		memset(bytes, 0, Min(capacity, PGRAC_CONTROL_BINDING_BYTES));
	if (overlap || bytes == NULL || capacity != PGRAC_CONTROL_BINDING_BYTES
		|| !binding_valid(binding))
		return false;
	memcpy(bytes, "PGCB", 4);
	binding_put(bytes, 4, 1, 2);
	binding_put(bytes, 6, PGRAC_CONTROL_BINDING_BYTES, 2);
	binding_put(bytes, 8, UINT32_C(0x01020304), 4);
	binding_put(bytes, 16, binding->system_identifier, 8);
	memcpy(bytes + 24, binding->storage_uuid, 16);
	memcpy(bytes + 40, binding->authority_uuid, 16);
	binding_put(bytes, 56, binding->database_incarnation, 8);
	binding_put(bytes, 64, binding->node_id, 4);
	memcpy(bytes + 72, binding->operation_uuid, 16);
	memcpy(bytes + 88, binding->source_cold_sha256, 32);
	memcpy(bytes + 120, binding->target_qualification_sha256, 32);
	memcpy(bytes + 152, binding->migration_round_sha256, 32);
	memcpy(bytes + 184, binding->source_wal_state_sha256, 32);
	binding_put(bytes, 216, binding->migration_prepare_generation, 8);
	binding_put(bytes, 224, binding->migration_transition_epoch, 8);
	binding_put(bytes, 248, binding_crc(bytes), 4);
	return true;
}

bool
pgrac_control_binding_decode(const uint8 *bytes, size_t length, PgracControlBinding *out)
{
	PgracControlBinding decoded;
	bool overlap = binding_overlap(bytes, length, out, sizeof(*out));

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (overlap || out == NULL || bytes == NULL || length != PGRAC_CONTROL_BINDING_BYTES)
		return false;
	if (memcmp(bytes, "PGCB", 4) != 0 || binding_get(bytes, 4, 2) != 1
		|| binding_get(bytes, 6, 2) != PGRAC_CONTROL_BINDING_BYTES
		|| binding_get(bytes, 8, 4) != UINT32_C(0x01020304) || !binding_zero(bytes + 12, 4)
		|| !binding_zero(bytes + 68, 4) || !binding_zero(bytes + 232, 16)
		|| !binding_zero(bytes + 252, 4) || binding_get(bytes, 248, 4) != binding_crc(bytes))
		return false;
	memset(&decoded, 0, sizeof(decoded));
	decoded.system_identifier = binding_get(bytes, 16, 8);
	memcpy(decoded.storage_uuid, bytes + 24, 16);
	memcpy(decoded.authority_uuid, bytes + 40, 16);
	decoded.database_incarnation = binding_get(bytes, 56, 8);
	decoded.node_id = binding_get(bytes, 64, 4);
	memcpy(decoded.operation_uuid, bytes + 72, 16);
	memcpy(decoded.source_cold_sha256, bytes + 88, 32);
	memcpy(decoded.target_qualification_sha256, bytes + 120, 32);
	memcpy(decoded.migration_round_sha256, bytes + 152, 32);
	memcpy(decoded.source_wal_state_sha256, bytes + 184, 32);
	decoded.migration_prepare_generation = binding_get(bytes, 216, 8);
	decoded.migration_transition_epoch = binding_get(bytes, 224, 8);
	if (!binding_valid(&decoded))
		return false;
	*out = decoded;
	return true;
}

#if !defined(WIN32) && defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)           \
	&& defined(AT_FDCWD)
static bool
binding_path_valid(const char *path, size_t length)
{
	size_t start = 1;

	if (length < 2 || length >= MAXPGPATH || path[0] != '/')
		return false;
	for (size_t i = 1; i <= length; i++) {
		if (i == length || path[i] == '/') {
			size_t size = i - start;

			if (size == 0 || (size == 1 && path[start] == '.')
				|| (size == 2 && path[start] == '.' && path[start + 1] == '.'))
				return false;
			start = i + 1;
		}
	}
	return true;
}

static bool
binding_owned(const struct stat *st)
{
	return st->st_uid == geteuid() && (st->st_mode & (S_IWGRP | S_IWOTH)) == 0;
}

static PgracControlBindingResult
binding_open_error(void)
{
	switch (errno) {
	case ENOENT:
		return PGRAC_CONTROL_BINDING_MISSING;
	case ELOOP:
	case ENOTDIR:
	case EACCES:
		return PGRAC_CONTROL_BINDING_UNSAFE;
	default:
		return PGRAC_CONTROL_BINDING_IO_ERROR;
	}
}
#endif

PgracControlBindingResult
pgrac_control_binding_read(const char *pgdata, PgracControlBinding *out)
{
#if !defined(WIN32) && defined(O_NOFOLLOW) && defined(O_DIRECTORY) && defined(O_CLOEXEC)           \
	&& defined(AT_FDCWD)
	int data_fd = -1;
	int global_fd = -1;
	int file_fd = -1;
	struct stat st;
	uint8 bytes[PGRAC_CONTROL_BINDING_BYTES + 1];
	size_t length = pgdata != NULL ? strnlen(pgdata, MAXPGPATH) : 0;
	size_t used = 0;
	bool overlap = binding_overlap(pgdata, length + 1, out, sizeof(*out));
	PgracControlBindingResult result = PGRAC_CONTROL_BINDING_OK;

	if (out != NULL)
		memset(out, 0, sizeof(*out));
	if (overlap || out == NULL || pgdata == NULL || !binding_path_valid(pgdata, length))
		return PGRAC_CONTROL_BINDING_INVALID;
	data_fd = open(pgdata, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (data_fd < 0) {
		result = binding_open_error();
		goto done;
	}
	if (fstat(data_fd, &st) != 0) {
		result = PGRAC_CONTROL_BINDING_IO_ERROR;
		goto done;
	}
	if (!S_ISDIR(st.st_mode) || !binding_owned(&st)) {
		result = PGRAC_CONTROL_BINDING_UNSAFE;
		goto done;
	}
	global_fd = openat(data_fd, "global", O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
	if (global_fd < 0) {
		result = binding_open_error();
		goto done;
	}
	if (fstat(global_fd, &st) != 0) {
		result = PGRAC_CONTROL_BINDING_IO_ERROR;
		goto done;
	}
	if (!S_ISDIR(st.st_mode) || !binding_owned(&st)) {
		result = PGRAC_CONTROL_BINDING_UNSAFE;
		goto done;
	}
	file_fd = openat(global_fd, PGRAC_CONTROL_BINDING_NAME,
					 O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC | PG_BINARY);
	if (file_fd < 0) {
		result = binding_open_error();
		goto done;
	}
	if (fstat(file_fd, &st) != 0) {
		result = PGRAC_CONTROL_BINDING_IO_ERROR;
		goto done;
	}
	if (!S_ISREG(st.st_mode) || !binding_owned(&st)) {
		result = PGRAC_CONTROL_BINDING_UNSAFE;
		goto done;
	}
	if (st.st_size != PGRAC_CONTROL_BINDING_BYTES) {
		result = PGRAC_CONTROL_BINDING_INVALID;
		goto done;
	}
	while (used < sizeof(bytes)) {
		ssize_t n = read(file_fd, bytes + used, sizeof(bytes) - used);

		if (n < 0 && errno == EINTR)
			continue;
		if (n < 0) {
			result = PGRAC_CONTROL_BINDING_IO_ERROR;
			goto done;
		}
		if (n == 0)
			break;
		used += n;
	}
	if (used != PGRAC_CONTROL_BINDING_BYTES)
		result = PGRAC_CONTROL_BINDING_INVALID;
done:
	/* Decode only after every raw descriptor is closed, even on a close error. */
	if (file_fd >= 0 && close(file_fd) != 0 && result == PGRAC_CONTROL_BINDING_OK)
		result = PGRAC_CONTROL_BINDING_IO_ERROR;
	if (global_fd >= 0 && close(global_fd) != 0 && result == PGRAC_CONTROL_BINDING_OK)
		result = PGRAC_CONTROL_BINDING_IO_ERROR;
	if (data_fd >= 0 && close(data_fd) != 0 && result == PGRAC_CONTROL_BINDING_OK)
		result = PGRAC_CONTROL_BINDING_IO_ERROR;
	if (result == PGRAC_CONTROL_BINDING_OK && !pgrac_control_binding_decode(bytes, used, out))
		result = PGRAC_CONTROL_BINDING_INVALID;
	return result;
#else
	if (out != NULL)
		memset(out, 0, sizeof(*out));
	return PGRAC_CONTROL_BINDING_UNSUPPORTED;
#endif
}
